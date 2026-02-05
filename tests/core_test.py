import trio


async def child1():
    print("  child1: started! sleeping now...")
    await trio.sleep(7)
    print("  child1: exiting!")


async def child2():
    print("  child2: started! sleeping now...")
    await trio.sleep(3)
    print("  child2: exiting!")


async def parent():
    print("parent: started!")
    async with trio.open_nursery() as nursery:
        print("parent: spawning child1...")
        nursery.start_soon(child1)

        print("parent: spawning child2...")
        nursery.start_soon(child2)

        print("parent: waiting for children to finish...")
    print("parent: all done!")


class Tracer(trio.abc.Instrument):
    def __init__(self):
        self._sleep_time = None

    def before_run(self):
        print("!!! run started")

    def task_spawned(self, task):
        print(f"### new task spawned: {task.name}")

    def task_scheduled(self, task):
        print(f"### task scheduled: {task.name}")

    def before_task_step(self, task):
        print(f">>> about to run one step of task: {task.name}")

    def after_task_step(self, task):
        print(f"<<< task step finished: {task.name}")

    def task_exited(self, task):
        print(f"### task exited: {task.name}")

    def before_io_wait(self, timeout):
        if timeout:
            self._sleep_time = trio.current_time()
            print(f"### waiting for I/O for up to {timeout} seconds")
        else:
            print("### doing a quick check for I/O")

    def after_io_wait(self, timeout):
        if timeout and self._sleep_time is not None:
            duration = trio.current_time() - self._sleep_time
            print(f"### finished I/O check (took {duration} seconds)")
            self._sleep_time = None
        else:
            print("### finished a quick check for I/O")

    def after_run(self):
        print("!!! run finished")


trio.run(parent, instruments=[Tracer()])
