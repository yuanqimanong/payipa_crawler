from functools import wraps

import anyio
import anyio.from_thread
import anyio.to_thread

# 全局任务注册表，用于框架自动发现任务
_REGISTRY = []


class TaskContext:
    """
    任务上下文：包裹用户的同步函数，处理异步调度
    """

    def __init__(self, func, queue_name, max_workers=10):
        self.func = func
        self.queue_name = queue_name
        # 异步队列 (Buffer)
        self.send_stream, self.receive_stream = anyio.create_memory_object_stream(1000)
        # 限制并发数的信号量 (Bulkhead模式)
        self.limiter = anyio.Semaphore(max_workers)
        _REGISTRY.append(self)

    def push(self, *args, **kwargs):
        """
        【关键】这是给用户调用的同步方法 (Sync API)
        用户在同步函数里调用它，它会在底层通过 Portal 桥接到异步循环
        """
        # 这里的逻辑是：不管谁调用我，我都把它转交给 AnyIO 的主循环去执行 _async_push
        try:
            anyio.from_thread.run(self._async_push, args, kwargs)
        except RuntimeError:
            # 如果是在主协程外部调用（比如脚本入口），需要特殊处理或报错，
            # 这里为了Demo简单，假设都在运行时的上下文或通过外部入口注入
            print("❌ 错误：push必须在框架运行上下文中调用")

    async def _async_push(self, args, kwargs):
        """内部的异步推送逻辑"""
        # print(f"    [框架调度] 任务入队 -> {self.queue_name}")
        await self.send_stream.send((args, kwargs))

    async def _worker_loop(self):
        """内部的消费者循环"""
        print(f"🔧 [Worker] 启动监听: {self.queue_name}")
        async for item in self.receive_stream:
            args, kwargs = item
            # 获取信号量，限制并发线程数
            async with self.limiter:
                # 【核心魔法】把用户的同步函数扔到线程池去跑，不要阻塞我的异步循环！
                # print(f"    [线程池] 正在执行用户逻辑: {self.queue_name}")
                await anyio.to_thread.run_sync(self._execute_user_logic, args, kwargs)

    def _execute_user_logic(self, args, kwargs):
        """在线程中真正执行用户的代码"""
        try:
            self.func(*args, **kwargs)
        except Exception as e:
            print(f"❌ 用户代码报错 ({self.queue_name}): {e}")

    def attach(self, tg):
        tg.start_soon(self._worker_loop)


def crawler_task(queue_name, max_workers=5):
    """
    装饰器：用户只看到这个
    """

    def decorator(func):
        # 创建 TaskContext 实例代替原函数
        task = TaskContext(func, queue_name, max_workers)

        # 为了让用户能调用 func.push()，我们需要在这个 wrapper 上挂载 push
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper.push = task.push
        # 隐藏属性，框架用来启动
        wrapper._task_context = task
        return wrapper

    return decorator


class SpiderEngine:
    """
    引擎入口
    """

    @staticmethod
    def start(backend='trio'):
        print("🚀 引擎启动中...")
        try:
            anyio.run(SpiderEngine._main_entry, backend=backend)
        except KeyboardInterrupt:
            pass

    @staticmethod
    async def _main_entry():
        async with anyio.create_task_group() as tg:
            # 1. 自动发现并启动所有被装饰的任务
            for task_ctx in _REGISTRY:
                task_ctx.attach(tg)

            # 2. 保持运行 (在真实场景中这里会有更复杂的退出机制)
            # 这里我们通过无限等待来模拟守护进程，或者等待队列为空
            print("✅ 所有 Worker 已就绪，等待任务...")
            while True:
                await anyio.sleep(1)


import time
import random


# from framework import crawler_task, SpiderEngine (假设上面的代码保存为framework)

# 模拟一个解析任务
@crawler_task(queue_name="save_db", max_workers=2)
def step_save_data(url, title):
    print(f"💾 [入库] 正在写入数据库: {title}...")
    # 用户完全可以用阻塞的 time.sleep
    time.sleep(0.5)
    print(f"✅ [完成] {url} 数据已保存")


# 模拟一个下载任务
@crawler_task(queue_name="download", max_workers=5)
def step_download(url):
    print(f"🌐 [下载] 开始请求: {url}")

    # 模拟网络阻塞，用户不需要知道这就是在线程里跑的
    time.sleep(random.uniform(0.5, 1.5))

    # 模拟解析出了标题
    title = f"Page Title for {url.split('//')[-1]}"

    # 【重点】用户直接调用 .push()，像调用普通函数一样
    # 框架会在底层把它转回异步消息
    step_save_data.push(url, title)


# 模拟入口
@crawler_task(queue_name="seed")
def start_requests():
    urls = [f"http://site-{i}.com" for i in range(1, 10)]
    for url in urls:
        step_download.push(url)


# 启动
if __name__ == '__main__':
    # 用户需要在某处注入种子任务
    # 但由于我们的 push 依赖 anyio 循环运行，所以这里需要一点小技巧：
    # 我们可以定义一个特殊的“启动钩子”，或者简单地让引擎启动后，我们在内部触发

    # 为了演示简单，我修改一下 Engine 的启动逻辑，允许传入一个 init 函数

    async def main_logic():
        # 手动注入种子，注意：这里是在 async 上下文中，
        # 如果要调用同步的 push，我们需要用 to_thread 或者直接调用底层的 _async_push
        # 为了给用户“全同步”的体验，我们在 Engine 内部做一个引导任务即可。

        # 这里模拟用户逻辑中最开始的触发：
        # 在框架启动的 TaskGroup 里，我们专门开一个协程来运行 start_requests
        await anyio.to_thread.run_sync(start_requests)


    # 稍微修改一下 Engine 以支持这个 Demo 的启动方式
    # 在真实框架中，这部分通常封装在 Engine.start(seed_task=start_requests)

    print("--- 用户脚本开始 ---")


    async def boot():
        async with anyio.create_task_group() as tg:
            # 1. 启动所有队列监听
            for task_ctx in _REGISTRY:
                task_ctx.attach(tg)

            # 2. 启动种子任务 (在线程池中运行，不阻塞主循环)
            tg.start_soon(anyio.to_thread.run_sync, start_requests)


    anyio.run(boot, backend='trio')
