#!/usr/bin/env python
# Copyright 2020 Ryan Svihla
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import uuid
from cassandra.cluster import Cluster
from cassandra.cqlengine.query import BatchQuery
from faker import Faker

parser = argparse.ArgumentParser(description='simple load generator for cassandra')
parser.add_argument('--hosts', default='127.0.0.1', type=string, help='comma separated list of hosts to use for contact points')
parser.add_argument('--port', default=9042, type=int, help='port to connect to')
parser.add_argument('--trans', default=1000000, type=int, help='number of transactions') 
parser.add_argument('--inflight', default=25, type=int, help='number of operations in flight') 
parser.add_argument('--errors', default=-1, type=int, help='number of errors before stopping. default is unlimited') 
args = parser.parse_args()
fake = Faker()

try:
    cluster = Cluster(args.hosts, port=args.port)
    session = cluster.connect()
    # setup tables
    session.execute("CREATE KEYSPACE IF NOT EXISTS my_key WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}")
    session.execute("CREATE TABLE IF NOT EXISTS my_key.my_table (id uuid, string, name text, address text, state text, zip text, balance float, PRIMARY KEY(id))")
    # setup queries
    insert = session.prepare("INSERT INTO my_key.my_table (id, name, address, state, zip, balance) VALUES (?, ?, ?, ?, ?, ?)")
    insert_rollup = session.prepare("INSERT INTO my_key.my_table (state, balance) VALUES (?, ?)")
    row_lookup = session.prepare("SELECT * FROM my_key.my_table WHERE id = ?")
    rollup = session.prepare("SELECT balance FROM my_key.my_table_by_state WHERE state = ?")
    threads = []
    ids = []
    error_counter = 0
    query = None
    params = []
    ids = []
    get_id = lambda _: ids[random.randomint(0, len(ids))]
    for i in range(args.trans):
        chance = random.randomint(1, 100)
        if chance > 0 and chance < 50:
            new_id = uuid.uuid4()
            ids.append(new_id)
            state = fake.state()
            query = BatchState()
            query.add(insert.bind([new_id, fake.name(), fake.address(), state, fake.zip(), random.randomint(1, 50000)]
            query.add(insert_rollup.bind([state]))
        elif chance > 50 and chance < 75:
            query = row_lookup.bind(get_id()]
        elif chance > 75:
            query = rollup.bind([fake.state()])
        threads.add(session.execute_async(query))
        if i % args.inflight == 0:
            for t in threads:
                try:
                    t.result() #we don't care about result so toss it
                except Exception as e:
                    print("unexpected exception %s" % e)
                    if args.errors > 0:
                        error_counter = error_counter + 1
                        if error_counter > args.errors:
                            print("too many errors stopping. Consider raising --errors flag if this happens more quickly than you'd like")
                            break

