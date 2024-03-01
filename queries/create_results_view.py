from dataclasses import dataclass

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    UNIVERSE_SUBSET,
)
from diamond_miner.queries.fragments import cut_ipv6, date_time
from diamond_miner.queries.query import Query, StoragePolicy, results_table, results_view, bgp_table
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateResultsView(Query):
    """Create the table used to store the measurement results from the prober."""

    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6
    storage_policy: StoragePolicy = StoragePolicy()

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        #最长前缀匹配
        return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {results_view(measurement_id)}
        ENGINE = MergeTree() ORDER BY probe_dst_addr POPULATE AS
        SELECT t1.ip_asn, t1.probe_dst_addr,t1.probe_src_port, t1.reply_src_addr, t1.probe_ttl, t1.asn
        FROM (
            SELECT bgp.ip_asn, results.probe_dst_addr,results.probe_src_port, results.reply_src_addr, results.probe_ttl, results.asn, bgp.len,
                ROW_NUMBER() OVER (PARTITION BY results.probe_dst_addr,results.reply_src_addr, results.probe_src_port ORDER BY bgp.len DESC) AS row_num
            FROM {results_table(measurement_id)} results
            CROSS JOIN {bgp_table(measurement_id)} bgp
            WHERE results.probe_dst_addr >= tupleElement(bgp.ipv6_range,1) AND results.probe_dst_addr <= tupleElement(bgp.ipv6_range,2)
        ) AS t1
        WHERE t1.row_num = 1
        """