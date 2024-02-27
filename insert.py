import os
from dataclasses import dataclass
from typing import Iterable, Iterator, Tuple

from pych_client import ClickHouseClient

from diamond_miner.defaults import (
    DEFAULT_FAILURE_RATE,
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.format import format_ipv6
from diamond_miner.generators.standalone import split_prefix
from diamond_miner.queries import InsertMDAProbes
from diamond_miner.queries.query import Query,ProbesQuery ,probes_table,pre_probes_table
from diamond_miner.subsets import subsets_for
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class InsertProbes(Query):
    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"INSERT INTO {probes_table(measurement_id)} FORMAT JSONCompactEachRow"

@dataclass(frozen=True)
class InsertPreProbes(Query):
    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"INSERT INTO {pre_probes_table(measurement_id)} FORMAT JSONCompactEachRow"

@dataclass(frozen=True)
class InsertByPreProbes(ProbesQuery):
    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""
        INSERT INTO {probes_table(measurement_id)}
        SELECT
            pre.probe_protocol,
            pre.probe_dst_prefix,
            pre.probe_ttl,
            if(p.cumulative_probes > pre.sum_cumulative_probes AND p.round = pre.round - 1, p.cumulative_probes, pre.sum_cumulative_probes) AS cumulative_probes,
            pre.round
        FROM (
            SELECT
                probe_protocol,
                probe_dst_prefix,
                probe_ttl,
                SUM(cumulative_probes) AS sum_cumulative_probes,
                round
            FROM {pre_probes_table(measurement_id)}
            WHERE round = {self.round_eq}
            GROUP BY
                probe_protocol,
                probe_dst_prefix,
                probe_ttl,
                round
        ) pre
        ANY LEFT JOIN {probes_table(measurement_id)} p ON pre.probe_protocol = p.probe_protocol
        AND pre.probe_dst_prefix = p.probe_dst_prefix
        AND pre.probe_ttl = p.probe_ttl
        AND pre.round - 1 = p.round
        """


def insert_probe_counts(
    client: ClickHouseClient,
    measurement_id: str,
    round_: int,
    # prefix (/32 or / 128 if nothing specified), protocol, ttls, n_probes
    prefixes: Iterable[Tuple[str, str, Iterable[int], int]],
    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,
) -> None:
    """
    >>> from diamond_miner.test import client, create_tables
    >>> from diamond_miner.queries import GetProbes
    >>> create_tables(client, "test_probe_counts")
    >>> insert_probe_counts(client, "test_probe_counts", 1, [("8.8.0.0/22", "icmp", range(2, 5), 6)])
    >>> rows = sorted(GetProbes(round_eq=1).execute(client, "test_probe_counts"), key=lambda x: x["probe_dst_prefix"])
    >>> len(rows)
    4
    >>> row = rows[0]
    >>> row["probe_dst_prefix"]
    '::ffff:8.8.0.0'
    >>> sorted(row["probes_per_ttl"])
    [[2, 6], [3, 6], [4, 6]]
    >>> row = rows[1]
    >>> row["probe_dst_prefix"]
    '::ffff:8.8.1.0'
    >>> sorted(row["probes_per_ttl"])
    [[2, 6], [3, 6], [4, 6]]
    """

    def gen() -> Iterator[bytes]:
        for prefix, protocol, ttls, n_probes in prefixes:
            protocol = PROTOCOLS[protocol]  # type: ignore
            for af, subprefix, subprefix_size in split_prefix(
                prefix, prefix_len_v4, prefix_len_v6
            ):
                yield "\n".join(
                    f'[{protocol},"{format_ipv6(subprefix)}",{ttl},{n_probes},{round_}]'
                    for ttl in ttls
                ).encode()

    InsertProbes().execute(client, measurement_id, data=gen())


def insert_mda_probe_counts(
    client: ClickHouseClient,
    measurement_id: str,
    previous_round: int,
    adaptive_eps: bool = False,
    target_epsilon: float = DEFAULT_FAILURE_RATE,
    concurrent_requests: int = (os.cpu_count() or 2) // 2,
) -> None:
    # TODO: set filter_partial and filter_virtual to false?
    query = InsertMDAProbes(
        adaptive_eps=adaptive_eps,
        round_leq=previous_round,
        filter_partial=True,
        filter_virtual=True,
        filter_inter_round=True,
        target_epsilon=target_epsilon,
    )
    subsets = subsets_for(query, client, measurement_id)
    query.execute_concurrent(
        client, measurement_id, subsets=subsets, concurrent_requests=concurrent_requests
    )
