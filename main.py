import multiprocessing
from etl import ETL
from manager import Manager
from mq_manager import MQManager
from worker import Worker
from client import Client


class ETLProcess(multiprocessing.Process):
    def __init__(self, filename: str, blocks_number: int) -> None:
        self.etl = ETL(filename, blocks_number)
        super().__init__()

    def run(self) -> None:
        self.etl.send_all()
        print("ETL finished")


class ManagerProcess(multiprocessing.Process):
    def __init__(self) -> None:
        self.manager = Manager()
        super().__init__()

    def run(self) -> None:
        self.manager.proceed_requests()
        print("Manager finished")


class WorkerProcess(multiprocessing.Process):
    def __init__(self, worker_id: int) -> None:
        self.worker_id = worker_id
        self.worker = Worker(worker_id)
        super().__init__()

    def run(self) -> None:
        self.worker.proceed_requests()
        print(f"Worker {self.worker_id} finished")


def main():
    workers_number = int(input("Please input number of workers: "))

    etl_process = ETLProcess("vienna-streets.geojson", workers_number)
    manager_process = ManagerProcess()

    etl_process.start()
    manager_process.start()

    worker_processes: list[WorkerProcess] = []
    for i in range(workers_number):
        process = WorkerProcess(i)
        process.start()
        worker_processes.append(process)

    client = Client()

    etl_process.join()
    client.run_console()
    manager_process.join()

    for process in worker_processes:
        process.join()


if __name__ == "__main__":
    try:
        main()
    except:
        MQManager.delete_all_queues()
