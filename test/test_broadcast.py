import unittest
import asyncio
from signals import Broadcast, ROOT, ConnectionEnded, PendingConnection, Sender

class TestBroadcast(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.broadcast = ROOT
        self.child_broadcast = self.broadcast.create_sub_broadcast('test')

    def test_subscribe_and_emit(self):
        async def callback(value):
            self.assertEqual(value, 'test_value')

        async def test_coroutine():
            self.broadcast.subscribe(callback, event='test_event')
            await self.broadcast.emit('test_event', 'test_value')

        self.loop.run_until_complete(test_coroutine())

    def test_wait_for(self):
        async def test_coroutine():
            future = self.broadcast.wait_for('test_event')
            await self.broadcast.emit('test_event', 'test_value')
            result = await future
            self.assertEqual(result, 'test_value')

        self.loop.run_until_complete(test_coroutine())

    def test_subscribe_and_emit_multiple_callbacks(self):
        async def callback1(value):
            self.assertEqual(value, 'test_value')

        async def callback2(value):
            self.assertEqual(value, 'test_value')

        async def test_coroutine():
            self.broadcast.subscribe(callback1, event='test_event')
            self.broadcast.subscribe(callback2, event='test_event')
            await self.broadcast.emit('test_event', 'test_value')

        self.loop.run_until_complete(test_coroutine())

    def test_wait_for_value_passing(self):
        async def f1():

            while True:
                v = await self.broadcast.wait_for('t2', timeout=1.0)
                # await asyncio.sleep(0.01)
                await self.broadcast.emit('t1', v + 1)
                # print(v)
                if v > 2000:
                    self.assertEqual(True, True)
                    break
        async def f2():
            await self.broadcast.emit('t2', 0)
            while True:
                v = await self.broadcast.wait_for('t1', timeout=1.0)
                # await asyncio.sleep(0.01)
                await self.broadcast.emit('t2', v + 1)
                # print(v)
                if v > 2000:
                    self.assertEqual(True, True)
                    break

        async def signaler():
            await asyncio.sleep(0.01)
            await self.broadcast.emit('t1', 0)

        async def test_coroutine():
            async with asyncio.TaskGroup() as t:
                t.create_task(f1())
                t.create_task(f2())
                t.create_task(signaler())
        self.loop.run_until_complete(test_coroutine())

    def test_bidirectional_communication(self):
        async def f1():
            await asyncio.sleep(0.5)
            fut = await self.broadcast.create_connection("test_conn")
            sender: Sender[int, int] = await fut
            i = 0
            self.assertEqual(sender.message, None, "Message should be None as f2 send None")
            excepted = False
            for _ in range(11):
                try:
                    await sender.send(i+1)
                    sender = await sender.wait_for_recv()
                    i = sender._message
                except ConnectionEnded:
                    excepted = True
                    break
            self.assertEqual(excepted, True, "A ConnectionEnded error should have caused")

        async def f2():
            pending_conn: PendingConnection = await self.broadcast.wait_for('test_conn', timeout=1)
            fut = await pending_conn.establish(None)
            sender: Sender[int, int] = await fut
            for i in range(10):
                message: int = sender.message
                await sender.send(message + 1)
                sender = await sender.wait_for_recv()
            await sender.end()
            self.assertEqual(sender.message, 21, "Result should be 20 after 10 2-way communication and 1 result send back from f1")

        async def test_coroutine():
            async with asyncio.TaskGroup() as t:
                t.create_task(f1())
                t.create_task(f2())
        self.loop.run_until_complete(test_coroutine())



    def test_parent_to_child_propagation(self):
        async def child_callback(value):
            self.assertEqual(value, 'test_value')

        async def test_coroutine():
            self.child_broadcast.subscribe(child_callback, event='test_event')
            await self.broadcast.emit('test_event', 'test_value')

        self.loop.run_until_complete(test_coroutine())

    def test_child_to_parent_propagation(self):
        async def parent_callback(value):
            self.assertEqual(value, 'test_value')

        async def test_coroutine():
            self.broadcast.subscribe(parent_callback, event='test_event')
            await self.child_broadcast.emit('test_event', 'test_value')

        self.loop.run_until_complete(test_coroutine())

if __name__ == '__main__':
    unittest.main()