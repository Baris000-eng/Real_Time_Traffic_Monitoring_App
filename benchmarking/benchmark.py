import sys
import json
import statistics
import redis
import matplotlib.pyplot as plt

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
RESULTS_FILE = "benchmark_results.json"


def measure(label):
    delays = [float(d) for d in r.lrange("delay_log", 0, -1)]

    if not delays:
        print("No data in delay_log")
        return

    avg = statistics.mean(delays)
    print(f"{label}: {len(delays)} samples, avg {avg:.2f} ms")

    try:
        with open(RESULTS_FILE) as f:
            results = json.load(f)
    except FileNotFoundError:
        results = {}

    results[label] = avg

    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)


def plot():
    with open(RESULTS_FILE) as f:
        results = json.load(f)

    plt.bar(results.keys(), results.values())
    plt.xlabel("Number of taxis")
    plt.ylabel("Avg delay (ms)")
    plt.title("Latency vs Load (T2 - T1)")
    plt.tight_layout()
    plt.savefig("latency_graph.png")
    plt.show()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        measure(sys.argv[1])
    else:
        plot()