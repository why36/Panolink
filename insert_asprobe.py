import os
from dataclasses import dataclass
from pych_client import ClickHouseClient
from collections import defaultdict
from typing import Iterable, Iterator, Tuple

from diamond_miner.defaults import (
    DEFAULT_FAILURE_RATE,
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    PROTOCOLS,
    UNIVERSE_SUBSET,
)
from diamond_miner.format import format_ipv6
from diamond_miner.generators.standalone import split_prefix
from diamond_miner.insert import InsertPreProbes,InsertByPreProbes
from diamond_miner.queries.query import Query, probes_table, asprobes_table, group_mapping
from diamond_miner.subsets import subsets_for
from diamond_miner.typing import IPNetwork
## TODO : rewrite this function using asprobes inner join group_mapping
def insert_as_probe(client: ClickHouseClient, 
                    measurement_id: str, 
                    candidates: dict,
                    round_: int,
                    prefix_len_v4: int = DEFAULT_PREFIX_LEN_V4,
                    prefix_len_v6: int = DEFAULT_PREFIX_LEN_V6,):
    def gen_asprobes() -> Iterator[bytes]:
        as_probe_results = client.json(f"""SELECT asp.probe_protocol, asp.group_id, asp.probe_ttl, asp.cumulative_probes, asp.round, groupArray(gr.ip_asn) as target_asns
                                       FROM {asprobes_table(measurement_id)} as asp
                                       INNER JOIN {group_mapping(measurement_id)} as gr 
                                       USING group_id
                                       where round = {round_}
                                       GROUP BY asp.probe_protocol, asp.group_id, asp.probe_ttl, asp.cumulative_probes, asp.round""")
        for result in as_probe_results:
            protocol = int(result["probe_protocol"])
            asn = result["group_id"]
            probe_ttl = int(result["probe_ttl"])
            cumulative_probes = int(result["cumulative_probes"])
            round = int(result["round"])
            target_asns = result["target_asns"]

            for target_asn in target_asns:
                curr_target_probenum = int(cumulative_probes) // len(target_asns)
                curr_prefix_probenum = int(curr_target_probenum) // len(candidates[target_asn])
                for prefix in candidates[target_asn]:
                    if prefix.prefixlen < 24:
                        curr_prefix_probenum_each = curr_prefix_probenum // len(list(prefix.subnets(new_prefix=24)))
                    else:
                        curr_prefix_probenum_each = curr_prefix_probenum
                    if curr_prefix_probenum_each < 1:
                        continue

                    subprefixes = [subprefix for af, subprefix, subprefix_size in split_prefix(str(prefix), prefix_len_v4, prefix_len_v6)]

                    yield "\n".join(
                        f'[{protocol},"{format_ipv6(subprefix)}",{probe_ttl},{curr_prefix_probenum_each},{round}]'
                        for subprefix in subprefixes
                    ).encode()

    InsertPreProbes().execute(client, measurement_id, data=gen_asprobes())
    InsertByPreProbes(round_eq=round_).execute(client, measurement_id)

