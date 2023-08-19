import json
from multiprocessing import Process
import time

from websockets.sync.client import connect

n_threads = 5


def run_thread(n):
    with connect("wss://fstream.binance.com/ws/btcusdt@bookTicker") as websocket:
        with open(f'./out_mp/stream_{n}.csv', 'w') as f:
            f.write('arrival_time,event_time,updateId\n')
            start_time = time.time()
            current_time = start_time
            while current_time - start_time <= 60:
                message = websocket.recv()
                current_time = time.time()
                parsed = json.loads(message)
                f.write(f"{current_time * 1000},{parsed['E']},{parsed['u']}\n")


if __name__ == "__main__":
    threads = []
    for i in range(n_threads):
        t = Process(target=run_thread, args=(i,))
        t.start()
        threads.append(t)
    for i in range(n_threads):
        threads[i].join()
