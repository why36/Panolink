from collections import defaultdict
import copy
from pych_client import ClickHouseClient
from ipaddress import IPv4Network
from diamond_miner.queries.query import Query, results_table,results_view, group_mapping
import subprocess

def get_subgraph(client: ClickHouseClient, measurement_id: str):
    middle_asns = client.text(f"select distinct asn from {results_table(measurement_id)} where reply_icmp_type = 11 and asn!='-1'").split('\n')
    for asn in middle_asns:
        edges = client.json(
            f"""WITH SubQuery AS (
                SELECT DISTINCT probe_dst_addr, probe_ttl
                FROM {results_view(measurement_id)}
                WHERE asn = '{asn}'
            )
            , JoinedResults AS (
                SELECT ot.probe_dst_addr, ot.probe_ttl, ot.asn,ot.ip_asn
                FROM {results_view(measurement_id)} AS ot
                ASOF INNER JOIN SubQuery AS sq ON ot.probe_dst_addr = sq.probe_dst_addr AND ot.probe_ttl >= sq.probe_ttl
                where ot.asn != '-1'
            )
            , Sequences AS (
                SELECT 
                    probe_dst_addr,
                    arrayConcat(
                        arrayMap(
                            i -> asn_sequence[i], arraySort(i -> ttl_sequence[i], range(1, length(ttl_sequence) + 1))),
                            arraySlice(ip_asn_sequence, -1))  
                    AS asn_sorted_sequence
                FROM (
                    SELECT 
                        probe_dst_addr,
                        groupArray(probe_ttl) as ttl_sequence,
                        groupArray(asn) as asn_sequence,
                        groupArray(ip_asn) as ip_asn_sequence
                    FROM 
                        JoinedResults
                    GROUP BY 
                        probe_dst_addr
                ) 
            )
            SELECT 
                deduped_sequence,
                count(*) as frequency
            FROM (
                SELECT 
                    arrayFilter(i -> (i = 1) OR (asn_sorted_sequence[i] != asn_sorted_sequence[i - 1]), 
                                range(1, length(asn_sorted_sequence)+1)) as filtered_indices,
                    arrayMap(i -> asn_sorted_sequence[i], filtered_indices) as deduped_sequence
                FROM Sequences
            )
            GROUP BY deduped_sequence
            """)
        #construct_subgraph(edges,asn)
        generate_mapping(client, measurement_id, edges, asn)
        

def generate_mapping(client: ClickHouseClient, measurement_id: str,leaf_paths,src_asn):
    statement = f"""INSERT INTO {group_mapping(measurement_id)} VALUES """
    leaf_paths = [t['deduped_sequence'] for t in leaf_paths if t['frequency'] > 1]
    print(leaf_paths)
    start_node_to_group = {}
    leaf_to_group = {}
    group_id = 1

    for path in leaf_paths:
        if len(path) < 2:
            leaf_to_group[path[0]] = src_asn + '.' + str(group_id)
            group_id += 1
            continue
        leaf = path[-1]  # 叶子节点是路径的最后一个元素
        # 如果路径长度大于1，则取第二个节点作为起始节点，否则取None
        start_node = path[1] if len(path) > 1 else None  
        if start_node not in start_node_to_group:
            start_node_to_group[start_node] = src_asn + '.' + str(group_id)
            group_id += 1
        
        leaf_to_group[leaf] = start_node_to_group[start_node]
    ret = [f"('{src_asn}', '{k}', '{v}')" for k,v in leaf_to_group.items()]
    statement += ",".join(ret)
    client.json(statement)      



## TODO : rewrite this function return candidates only
def calc_weights(client: ClickHouseClient, measurement_id : str, input_prefix : str):
    prefix_ip = IPv4Network(input_prefix)
    entrys = client.json(f"SELECT * FROM bgp__{measurement_id}".replace("-", "_"))
    candidates = defaultdict(list)  
    for entry in entrys:
        curr_prefix = IPv4Network(entry['prefix'] + '/' + str(entry['len']))
        curr_asn = entry['ip_asn']
        if curr_prefix.subnet_of(prefix_ip):
            candidates[curr_asn].append(curr_prefix)
        elif prefix_ip.subnet_of(curr_prefix):
            candidates[curr_asn].append(prefix_ip)
    return candidates




