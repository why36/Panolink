import time
from dataclasses import fields
from logging import Logger
from types import TracebackType
from typing import Any, Dict, Optional, Type
import subprocess


def lookup_dict(measurement_id: str) -> str:
    return f"lookup_dict__{measurement_id}".replace("-", "_")

def bdrmap_table(measurement_id: str) -> str:
    return f"bdrmap__{measurement_id}".replace("-", "_")

def results_table(measurement_id: str) -> str:
    return f"results__{measurement_id}".replace("-", "_")

def get_result_path(path, round):
    return str(path) + "_" + str(round)

def run_process_dminer(dminer_path):
    subprocess.call("/bin/bash /home/hongyu/diamond-miner/process_dminer.sh " + dminer_path,shell=True)

def prepare_bdrmapit(client,measurement_id):
    client.json(f"CREATE DATABASE {bdrmap_table(measurement_id)}")
    client.json(f"""create table {bdrmap_table(measurement_id)}.annotation
                    (
                        router String,
                        asn    String
                    )
                    ENGINE MergeTree
                    ORDER BY router;
                """)
    client.json(f"""CREATE DICTIONARY {lookup_dict(measurement_id)}
                    (
                        router String,
                        asn    String
                    )
                    PRIMARY KEY router 
                    SOURCE(CLICKHOUSE(TABLE 'annotation' DB '{bdrmap_table(measurement_id)}'))
                    LIFETIME(3)
                    LAYOUT(IP_TRIE);""")


def run_bdrmapit(client,measurement_id,round):
    print("???")
    subprocess.run(f'printf "\\n\\"/home/hongyu/diamond-miner/results.csv_{round}_processed.warts\\"" >> /home/hongyu/wartsfile_panolink', shell=True, check=True)
    print("!!!") 
    subprocess.call("sudo -u hongyu /usr/local/anaconda3/bin/conda run -n bdrmapit bdrmapit json -c /home/hongyu/bdrmapit/config.json -s /home/hongyu/diamond-miner/output.db ",shell=True)
    subprocess.call("sudo docker cp /home/hongyu/diamond-miner/output.db  3d45:/var/lib/clickhouse/user_files ",shell=True)
    client.json(f"DROP DATABASE {bdrmap_table(measurement_id)}")
    client.json(f"CREATE DATABASE {bdrmap_table(measurement_id)} ENGINE = SQLite('output.db')")
    time.sleep(5)
    client.json(f"ALTER TABLE {results_table(measurement_id)} update asn = dictGetOrDefault('{lookup_dict(measurement_id)}','asn',IPv4StringToNumOrDefault(SUBSTRING(toString(reply_src_addr), 8)),'-1') where 1")
    time.sleep(5)


def common_parameters(from_dataclass: Any, to_dataclass: Any) -> Dict[str, Any]:
    to_params = {field.name for field in fields(to_dataclass)}
    return {
        field.name: getattr(from_dataclass, field.name)
        for field in fields(from_dataclass)
        if field.name in to_params
    }


class Timer:
    """A very simple timer for profiling code blocks."""

    start_time = None
    total_time = 0

    def start(self) -> None:
        self.start_time = time.time_ns()

    def stop(self) -> None:
        if self.start_time:
            self.total_time += time.time_ns() - self.start_time
            self.start_time = None

    def clear(self) -> None:
        self.start_time = None
        self.total_time = 0

    @property
    def total_ms(self) -> float:
        return self.total_time / 10 ** 6


class LoggingTimer:
    """A very simple timer for logging the execution time of code blocks."""

    def __init__(self, logger: Logger, prefix: str = ""):
        self.logger = logger
        self.prefix = prefix
        self.timer = Timer()

    def __enter__(self) -> None:
        self.logger.info(self.prefix)
        self.timer.clear()
        self.timer.start()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.timer.stop()
        self.logger.info("%s time_ms=%s", self.prefix, self.timer.total_ms)
