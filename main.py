import asyncio
import json
import logging
import os
import threading
import time
import traceback
from queue import Queue, Empty
from typing import Union
import websocket
import rel
import time

import aiortc
import av
import requests
from aiohttp import web
from aiortc import MediaStreamTrack, RTCSessionDescription, RTCPeerConnection, RTCRtpSender
from aiortc.mediastreams import MediaStreamError
from av.frame import Frame
from av.packet import Packet
from pyee.asyncio import AsyncIOEventEmitter


class MediaFrameHolder:
    def __init__(self, url: str) -> None:
        self.audio_holder = FrameHolder('audio', url)
        self.video_holder = FrameHolder('video', url)


class FrameHolder:
    def __init__(self, kind, url: str) -> None:
        self._kind = kind
        # 视频url
        self._url = url
        self._is_net_file = url.startswith("http")
        # 如果是http的视频，先缓存到本地。否则av打开网上的视频，很久不读之后会有问题，造成画面卡死
        if self._is_net_file:
            self.local_filename = "video_temp/" + os.path.basename(url)
            # 如果目录不存在，就创建目录
            if not os.path.exists(os.path.dirname(self.local_filename)):
                os.makedirs(os.path.dirname(self.local_filename))
            # 发送 GET 请求获取视频二进制数据
            response = self._retry_get(url, 3)
            # 将二进制数据写入本地文件
            with open(self.local_filename, "wb") as f:
                f.write(response.content)
            self._container = av.open(self.local_filename)
        else:
            self._container = av.open(url)
        self.buf = []
        self.last_frame = None
        self._sampler = av.AudioResampler(
            format="s16",
            layout="stereo",
            rate=48000,
            frame_size=int(48000 * 0.02),
        )

    def _retry_get(self, url: str, count: int):
        for _ in range(count):
            try:
                return requests.get(url, timeout=(1, 3))
            except Exception:
                traceback.print_stack()
                logging.error("get video error: %s, %d", url, count)

    def clear(self):
        def close_resources():
            self._container.close()
            if self._is_net_file:
                os.remove(self.local_filename)

        try:
            close_resources()
        except Exception:
            traceback.print_stack()
            logging.warning("remove %s failed, try remove again 5s later", self.local_filename)
            timer = threading.Timer(5, close_resources)
            timer.start()  # 启动定时器

    def __iter__(self):
        return self

    def __next__(self):
        try:
            if self._kind == 'audio':
                audio_stream = self._container.streams.audio[0]
                frame = next(self._container.decode(audio_stream))
                for re_frame in self._sampler.resample(frame):
                    self.buf.append(re_frame)
            else:
                video_stream = self._container.streams.video[0]
                frame = next(self._container.decode(video_stream))
                self.buf.append(frame)
        except Exception:
            pass
        try:
            # Remove and return item at index (default last). fuck！！！
            f = self.buf.pop(0)
            self.last_frame = f
            return False, self.last_frame
        except Exception:
            self._container.close()
            return True, self.last_frame


class Sentinel:
    def __init__(self, player) -> None:
        self._player = player
        # 记录播放列表的游标
        self._play_list_cursor = 0
        self._thread = None
        self._sig_stop = False

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(
                name='sentinel',
                target=self.maintain_push_queue,
                args=()
            )
            self._thread.start()

    def stop(self):
        self._sig_stop = True

    # 下一个视频, 返回元组(idx，视频url), 如果是插播视频，idx为-1
    def next_video(self):
        # 有插播就先选择插播
        jump_in_list = self._player.jump_in_list
        jump_in = None
        try:
            jump_in = jump_in_list.get_nowait()
        except Empty:
            pass
        if jump_in:
            return jump_in

        # 没有插播的话要拿到当前播放的下一个
        play_list = self._player.playlist
        idx = self._play_list_cursor % len(play_list)
        url = play_list[idx]
        self._play_list_cursor += 1
        return url

    def maintain_push_queue(self):
        push_queue = self._player.push_queue
        while True:
            if self._sig_stop:
                break
            url = self.next_video()
            push_queue.put(MediaFrameHolder(url))
            print("successfully put to push_queue", url)


class PlayListTrack(MediaStreamTrack):
    def __init__(self, player, kind):
        super().__init__()
        self.kind = kind
        self._player = player
        self._queue = asyncio.Queue()
        self._start = None
        self._time = 0.
        self.fps = 25

    async def recv(self) -> Union[Frame, Packet]:
        if self.readyState != "live":
            raise MediaStreamError
        while self._player.push_queue.empty():
            await asyncio.sleep(0.01)

        if not self._player.push_queue.empty():
            media_frame_holder = self._player.push_queue.queue[0]

            if self.kind == 'audio':
                finished, data = next(media_frame_holder.audio_holder)
                data.pts = int(self._time * 48000)
                self._time += 0.02
                result = data
                if finished:
                    self._player.push_queue.get_nowait()
                    media_frame_holder.video_holder.clear()
            else:
                finished, data = next(media_frame_holder.video_holder)
                data.pts = int(self._time / data.time_base)
                self._time += 0.04
                result = data
                if finished:
                    self._player.push_queue.get_nowait()
                    media_frame_holder.audio_holder.clear()
        else:
            raise Exception('result is None')
        if result is None:
            raise Exception('result is None')
        data_time = float(result.pts * result.time_base)

        if self._start is None:
            self._start = time.time() - data_time
        else:
            wait = self._start + data_time - time.time()
            await asyncio.sleep(wait)

        return result

    def pause(self):
        pass

    def resume(self):
        pass

    def stop(self):
        super().stop()
        self._queue.empty()


class SegmentPlayer:
    def __init__(self, playlist: []) -> None:
        super().__init__()
        self.__audio = PlayListTrack(self, 'audio')
        self.__video = PlayListTrack(self, 'video')
        self.__loop = asyncio.get_event_loop()
        self.__thread = None
        self.__stop = False
        self.__sentinel = None
        # 循环播放队列
        self.__playlist = playlist
        # 插播队列
        self.__jump_in_list = Queue()
        # 等待推送队里了
        self.push_queue = Queue(maxsize=1)

    @property
    def is_stopped(self):
        return self.__stop

    @property
    def playlist(self) -> []:
        return self.__playlist

    @property
    def jump_in_list(self) -> Queue:
        return self.__jump_in_list

    @property
    def audio(self) -> PlayListTrack:
        """
        A :class:`aiortc.MediaStreamTrack` instance if the file contains audio.
        """
        return self.__audio

    @property
    def video(self) -> PlayListTrack:
        """
        A :class:`aiortc.MediaStreamTrack` instance if the file contains video.
        """
        return self.__video

    # 开始
    def start(self):
        if self.__sentinel is None:
            self.__sentinel = Sentinel(self)
            self.__sentinel.start()

    # 停止
    def stop(self):
        self.__stop = True
        self.audio.stop()
        self.video.stop()
        self.__sentinel.stop()

    def jump_in(self, url):
        self.__jump_in_list.put(url)


###########################################################
ROOT = os.path.dirname(__file__)

players = {}
pcs = set()
playlist = [
    "videos/40_16000_0.mp4",
    "videos/40_16000_1.mp4",
    "videos/40_16000_2.mp4",
    "videos/40_16000_3.mp4",
    "videos/40_16000_4.mp4",
    "videos/40_16000_5.mp4",
    "videos/40_16000_6.mp4",
    "videos/40_16000_7.mp4",
    "videos/40_16000_8.mp4",
    "videos/40_16000_9.mp4",
    "videos/40_16000_10.mp4",
]


async def index(request):
    content = open(os.path.join(ROOT, "index.html"), "r", encoding='utf-8').read()
    return web.Response(content_type="text/html", text=content)


async def offer(request):
    params = await request.json()
    conn_id = params["conn_id"]
    offer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])
    pc = RTCPeerConnection()
    pcs.add(pc)

    player = SegmentPlayer(playlist=playlist)
    players[conn_id] = player

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        print("Connection state is %s" % pc.connectionState)
        if pc.connectionState == "failed":
            await pc.close()
            pcs.discard(pc)
            player.stop()

    @pc.on("iceconnectionstatechange")
    async def on_iceconnectionstatechante():
        logging.info("ice connection stat change: %s", pc.iceConnectionState)
        if pc.iceConnectionState == "failed":
            await pc.close()
            pcs.discard(pc)
            player.stop()

    sender = pc.addTrack(player.video)
    pc.addTrack(player.audio)

    # aiortc.codecs.vpx.MIN_BITRATE = 2000000
    # aiortc.codecs.vpx.DEFAULT_BITRATE = 2000000
    # aiortc.codecs.vpx.MAX_BITRATE = 4000000

    aiortc.codecs.h264.MIN_BITRATE = 2000000
    aiortc.codecs.h264.DEFAULT_BITRATE = 2000000
    aiortc.codecs.h264.MAX_BITRATE = 4000000
    codecs = RTCRtpSender.getCapabilities("video").codecs
    transceiver = next(t for t in pc.getTransceivers() if t.sender == sender)
    transceiver.setCodecPreferences(
        [codec for codec in codecs if codec.mimeType == "video/H264"]
    )

    player.start()

    await pc.setRemoteDescription(offer)
    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)

    response = web.Response(
        content_type="application/json",
        text=json.dumps(
            {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type, "id": conn_id}
        ),
    )
    # 设置允许跨域请求
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


async def jump_in(request):
    params = await request.json()
    url = params["url"]
    id = params["id"]
    players[id].jump_in(url)
    response = web.Response(
        content_type="application/json",
        text=json.dumps(
            {"result": "ok"}
        ),
    )
    # 设置允许跨域请求
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


async def on_message(ws, message):
    json_message = json.loads(message)
    print(json_message)
    sdp = json_message["sdp"]
    type = json_message["type"]
    client_id = json_message["from"]

    conn_id = client_id
    offer = RTCSessionDescription(sdp=sdp, type=type)
    pc = RTCPeerConnection()
    pcs.add(pc)

    player = SegmentPlayer(playlist=playlist)
    players[conn_id] = player

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        print("Connection state is %s" % pc.connectionState)
        if pc.connectionState == "failed":
            await pc.close()
            pcs.discard(pc)
            player.stop()

    @pc.on("iceconnectionstatechange")
    async def on_iceconnectionstatechante():
        logging.info("ice connection stat change: %s", pc.iceConnectionState)
        if pc.iceConnectionState == "failed":
            await pc.close()
            pcs.discard(pc)
            player.stop()

    sender = pc.addTrack(player.video)
    pc.addTrack(player.audio)

    aiortc.codecs.h264.MIN_BITRATE = 2000000
    aiortc.codecs.h264.DEFAULT_BITRATE = 2000000
    aiortc.codecs.h264.MAX_BITRATE = 4000000
    codecs = RTCRtpSender.getCapabilities("video").codecs
    transceiver = next(t for t in pc.getTransceivers() if t.sender == sender)
    transceiver.setCodecPreferences(
        [codec for codec in codecs if codec.mimeType == "video/H264"]
    )

    player.start()

    await pc.setRemoteDescription(offer)
    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)
    ws.send(json.dumps({"sdp": pc.localDescription.sdp, "type": pc.localDescription.type, "to": conn_id}))


def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    print("Opened connection")

def init_websocket_client():
    # 启动socket服务 
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp("wss://cofi-ws.viseem.com?pid=server&client_type=server",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.run_forever()

if __name__ == '__main__':
    app = web.Application()

    # 添加静态资源处理
    # app.router.add_static('/static/', path='./static')
    
    t = threading.Thread(target=init_websocket_client)
    t.start()

    app.router.add_get("/", index)
    app.router.add_post("/offer", offer)
    app.router.add_post("/jumpin", jump_in)
    web.run_app(app, host='0.0.0.0', port=6300, ssl_context=None)
