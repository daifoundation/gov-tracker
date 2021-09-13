#  Copyright 2021 DAI Foundation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from snowflake.connector.cursor import SnowflakeCursor
import pandas as pd


def safe_max(sequence):
    if sequence:
        return max(sequence)
    else:
        return 0


# asynchronous queries execution using Snowflake
def async_queries(sf, all_queries):

    started_queries = []
    for i in all_queries:
        sf.execute(i['query'])
        started_queries.append(dict(
            qid=sf.sfqid,
            id=i['id']
        ))

    all_results = {}
    limit = len(started_queries)
    control = 0

    while limit != control:

        for i in started_queries:

            if i['id'] not in all_results.keys():

                try:
                    check_results = sf.execute("""
                        SELECT *
                        FROM table(result_scan('%s'))
                        """ % i['qid'])

                    if isinstance(check_results, SnowflakeCursor):
                        df = check_results.fetch_pandas_all()
                        result = df.where(pd.notnull(df), None).values.tolist()
                        all_results[i['id']] = result

                except Exception as e:
                    print(str(e))

        control = len(all_results.keys())

    return all_results
