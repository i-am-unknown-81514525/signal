from __future__ import annotations

import asyncio
from types import CoroutineType
import typing
from collections.abc import Callable, Coroutine
from typing import Any, Optional, overload, TypeVar

LOOP: asyncio.AbstractEventLoop = asyncio.get_event_loop()

class ConnectionInitialError(ConnectionError): ...

class ConnectionRejectedError(ConnectionInitialError): ...

class ConnectionConsumedError(ConnectionInitialError): ...

class ConnectionEnded(ConnectionError): ...

class DataError(Exception): ...

class DataNotSentError(Exception): ...

class DataAlreadySentError(Exception): ...

class AutoFillDict(dict):
    def __missing__(self, key):
        self[key] = []
        return self[key]


class PendingConnection[_T, _S]:
    def __init__(self, future: asyncio.Future[Sender[_T, _S]], *, check: Optional[Callable[[_T], bool]], loop: asyncio.AbstractEventLoop = LOOP):
        self._origin_future: asyncio.Future[Sender[_T, _S]] = future
        self._check: Optional[Callable[[_T], bool]] = check
        self._consumed: bool = False
        self._loop: asyncio.AbstractEventLoop = loop

    async def establish(self, resp: _T) -> asyncio.Future[Sender[_S, Any]]:
        fut: asyncio.Future = self._loop.create_future()
        r_resp: Sender[_T, Any] = Sender(fut, loop=self._loop, message=resp)
        if self._consumed:
            raise ConnectionConsumedError("Other part of program have already acquired the connection")
        if not self._check:
            self._consumed = True
            self._origin_future.set_result(r_resp)
            return fut
        if resp is None:
            try:
                self._check(resp)  # Check if the function is expecting it as a valid result
            except Exception as e:
                raise ConnectionRejectedError("Your request to establish the connection have been rejected because no data is provided") from e
        result = self._check(resp)
        if not result:
            raise ConnectionRejectedError("Your request to establish the connection have be rejected from the given data does not sastify the requirement by the check function")
        self._consumed = True
        self._origin_future.set_result(r_resp)
        return fut

class Sender[_R, _S]:
    def __init__(self, future: asyncio.Future, *, loop: asyncio.AbstractEventLoop = LOOP, message: _R):
        self._target_future: asyncio.Future[Sender[_S, Any]] = future
        self.sent: bool = False
        self._self_future: asyncio.Future = loop.create_future()
        self._loop: asyncio.AbstractEventLoop = loop
        self._message: _R = message

    @property
    def future(self):
        return self._self_future

    @property
    def message(self):
        return self._message

    def wait_for_recv(self, timeout: float | None = 30.0):
        if not self.sent:
            raise DataNotSentError("You must send a message before waiting to receive a response")
        return asyncio.wait_for(self.future, timeout=timeout)

    async def send(self, resp: _S) -> asyncio.Future:
        if self.sent:
            raise DataAlreadySentError("You cannot send message once before the other side response")
        r_resp: Sender[_S, Any] = Sender(self.future, loop=self._loop, message=resp)
        self.sent = True
        self._target_future.set_result(r_resp)
        return self.future

    async def end(self):
        self._target_future.set_exception(ConnectionEnded)

class Broadcast:
    def __init__(self, parent: Optional[Broadcast], name: str):
        self._name = name
        self._parent: Optional[Broadcast] = parent
        self._loop = self._parent.loop if self._parent else LOOP
        self._children: list[Broadcast] = []
        self._listener: dict[str | None, list[Callable[[Any], Coroutine[Any, Any, Any]]]] = AutoFillDict()
        self._temp_listener: list[tuple[str, asyncio.Future, Callable[[Any], bool]]] = []

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def subscribe(self, callback: Callable[[Any], Coroutine[Any, Any, Any]], event: Optional[str] = None):
        self._listener[event].append(callback)

    def wait_for[_V](self, event: str, timeout: float = 600.0, *, check: Callable[[_V], bool] = lambda *v: True) -> CoroutineType[Any, Any, _V]:
        future = self._loop.create_future()
        self._temp_listener.append((event, future, check))
        return asyncio.wait_for(future, timeout)

    async def self_emit(self, event: str, value: Any, *, tg: asyncio.TaskGroup):
        i = 0
        while i < len(self._temp_listener):
            try:
                r = self._temp_listener[i]
                if r[0] != event:
                    continue
                if not r[2](value):
                    continue
                r[1].set_result(value)
                self._temp_listener.pop(i)
                i -= 1
            finally:
                i += 1  # Python 3.8+
        for ev in self._listener[event]:
            tg.create_task(ev(value))
        for ev in self._listener[None]:
            tg.create_task(ev(value))

    async def parent_emit(self, event: str, value: Any, *, tg: asyncio.TaskGroup):
        if self._parent:
            await self._parent.self_emit(event, value, tg=tg)
            await self._parent.parent_emit(event, value, tg=tg)

    async def child_emit(self, event: str, value: Any, *, tg: asyncio.TaskGroup):
        for child in self._children:
            await child.self_emit(event, value, tg=tg)
            await child.child_emit(event, value, tg=tg)

    async def emit(self, event: str, value: Any):
        async with asyncio.TaskGroup() as t:
            await self.self_emit(event, value, tg=t)
            await self.parent_emit(event, value, tg=t)
            await self.child_emit(event, value, tg=t)

    def create_sub_broadcast(self, name: str) -> Broadcast:
        broadcast = Broadcast(parent=self, name=name)
        self._children.append(broadcast)
        return broadcast

    async def create_connection(self, event: str, *, check: Optional[Callable[[Any], bool]] = None) -> asyncio.Future[Sender]:
        origin_future: asyncio.Future[Sender] = self._loop.create_future()
        conn: PendingConnection = PendingConnection(origin_future, check=check, loop=self._loop)
        await self.emit(event=event, value=conn)
        return origin_future

local_broadcast_registry: dict[str, Broadcast] = {}

class BroadcastAdvertise(typing.NamedTuple):
    name: str
    broadcast: Broadcast

async def create_local_broadcast(name: str, advertise: bool = False) -> Broadcast:
    if name in local_broadcast_registry:
        broadcast: Broadcast = local_broadcast_registry[name]
    else:
        broadcast: Broadcast = Broadcast(parent=None, name=name)
    if advertise:
        await ROOT.emit("channel_adv", BroadcastAdvertise(name, broadcast))
    return broadcast

ROOT = Broadcast(parent=None, name='root')
local_broadcast_registry["root"] = ROOT