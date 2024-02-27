from dataclasses import dataclass

from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    UNIVERSE_SUBSET,
)
from diamond_miner.queries.fragments import cut_ipv6, date_time
from diamond_miner.queries.query import Query, StoragePolicy, group_mapping
from diamond_miner.typing import IPNetwork


@dataclass(frozen=True)
class CreateGroupMapping(Query):
    """Create the table used to store the measurement results from the prober."""

    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4
    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6
    storage_policy: StoragePolicy = StoragePolicy()

    def statement(
        self, measurement_id: str, subset: IPNetwork = UNIVERSE_SUBSET
    ) -> str:
        return f"""CREATE TABLE IF NOT EXISTS {group_mapping(measurement_id)} 
                (
                    asn String,  
                    ip_asn String,
                    group_id String
                ) ENGINE = MergeTree()
                ORDER BY (group_id);"""