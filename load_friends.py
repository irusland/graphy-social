import concurrent.futures
import json
import os
from typing import List
import requests

import networkx as nx
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
TOKEN = os.getenv('TOKEN')
MY_USER_ID = os.getenv('MY_USER_ID')

PHOTO_50 = 'photo_50'
SEX = 'sex'
BDATE = 'bdate'
ITEMS = 'items'
ID = 'id'
FIRST_NAME = 'first_name'
LAST_NAME = 'last_name'


def get_friends_(friend_id, session):
    url = f'https://api.vk.com/method/friends.get?access_token={TOKEN}&user_id={friend_id}&fields={PHOTO_50},{SEX}&v=5.131'
    r = session.get(url)
    if r.status_code != 200:
        raise Exception(r.status_code)

    j = r.json()
    # print(f'>>> {j=}')
    return j


def get_friends_concurrent(friends_ids, target_friends_callback):
    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        with requests.Session() as session:
            future_to_id = {executor.submit(get_friends_, friend_id, session): friend_id for friend_id in
                            friends_ids}
            for future in tqdm(concurrent.futures.as_completed(future_to_id),
                               total=len(friends_ids)):
                source_id = future_to_id[future]
                try:
                    target_friends = future.result()
                    target_friends_callback(source_id, target_friends)
                except Exception as exc:
                    print('%r generated an exception: %s' % (source_id, exc))
                    raise


def enrich_graph_with_friends(G, friends, source_id=MY_USER_ID, mutual_only=False, nodes_set=None) -> List[int]:
    friends_ids = []

    for friend in friends:
        print(f'>>> {friend=}')
        if friend == 'error':
            print(f'>>> {friends=}')
        id_ = friend[ID]
        if mutual_only:
            if id_ not in nodes_set:
                continue
        friends_ids.append(id_)
        G.add_node(id_, **friend)
        G.add_edge(source_id, id_)

    return friends_ids


def enrich_graph_with_friends_concurrent(G, friends, mutual_only=False, nodes_set=None):
    def target_friends_callback(source_id, target_friends):
        return enrich_graph_with_friends(G, target_friends, source_id, mutual_only, nodes_set)

    get_friends_concurrent(friends, target_friends_callback=target_friends_callback)


def main():
    G = nx.DiGraph()
    r = get_friends_(MY_USER_ID, requests.Session())
    fds = r['response']['items']
    # print(fds)
    friends_ids = enrich_graph_with_friends(G, fds)
    print(f'>>> {friends_ids=}')
    nodes_set = set(G.nodes)
    enrich_graph_with_friends_concurrent(G, friends=friends_ids, mutual_only=True, nodes_set=nodes_set)
    print(G.nodes)


if __name__ == '__main__':
    main()
