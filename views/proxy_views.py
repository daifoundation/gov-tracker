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

import os
from flask import render_template
from datetime import datetime, timezone

from connectors.sf import sf_connect
from utils.utils import async_queries
from utils.tables import link
from utils.polls import get_all_polls

# from graphs.proxy_graph import proxy_graph


def proxy_data_view(sf, proxy):

    # test snowflake connection and reconnect if necessary
    try:
        if sf.is_closed():
            sf = sf_connect()
        if sf.is_closed():
            raise Exception('Reconnection failed')

    except Exception as e:
        print(e)
        return dict(status='failure', data='Database connection error')

    try:

        # SQL injection prevention
        if not(len(proxy) == 42 and proxy[:2] == '0x'):
            return dict(status='failure', data='Unknown proxy')

        operations_query = f"""select timestamp, tx_hash, dstake, operation, yay, option, dapproval, decisive, order_index
                              from {os.getenv("MCDGOV_DB", "mcd_public")}.public.votes 
                              where lower(proxy) = '{proxy}' 
                              order by timestamp, operation; """

        titles_query = f"""select code, title from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.yays; """

        all_queries = [
            dict(query=operations_query, id='operations'),
            dict(query=titles_query, id='titles')
        ]

        # snowflake data ingestion

        sf_responses = async_queries(sf, all_queries)
        operations = sf_responses['operations']
        titles = sf_responses['titles']

        titles = {y[0]: y[1] for y in titles}
        titles['0x0000000000000000000000000000000000000000'] = 'Activate DSChief v1.2'

        polls_metadata = get_all_polls()

        proxy_operations = []
        x = []
        y = []
        labels = []
        stake = 0
        for operation in operations:

            stake += operation[2] or 0
            poll = operation[4]
            title = titles.get(operation[4], operation[4]) or ''

            if operation[3] == 'FINAL_CHOICE':
                votes = operation[5].split(',')
                if poll in polls_metadata:
                    options = '<br>'.join([str(polls_metadata[poll][1][vote]) if vote in polls_metadata[poll][1] else 'Not valid' for vote in votes])
                else:
                    options = '<br>'.join([str(vote) for vote in votes])
                poll_link = link(title, '/poll/%s' % poll, title) if poll else ''
            else:
                options = ''
                poll_link = link(title, '/yay/%s' % poll, title) if poll else ''

            if operation[3]:
                if operation[3] != 'FINAL_CHOICE':
                    ops = link(operation[3], 'https://ethtx.info/%s' % operation[1], '%s transaction' % operation[3], new_window=True)
                else:
                    ops = operation[3]
            else:
                ops = ''

            operation_row = [operation[0],
                            ops,
                            "{0:,.2f}".format(operation[2]) if operation[2] else '',
                            "{0:,.2f}".format(stake),
                            poll_link,
                            options,
                            'YES' if operation[7] else '',
                            operation[8]]

            proxy_operations.append(operation_row)

        operations_num = "{0:,d}".format(len(proxy_operations))

        # prepare output data
        operations_data = []
        for operation in proxy_operations:
            operations_data.append(dict(
                TIME=operation[0].strftime("%Y-%m-%d %H:%M:%S") + ' ' + str(operation[7]),
                OPERATION=operation[1],
                TRANSFER=operation[2],
                STAKE=operation[3],
                POLL=operation[4],
                OPTIONS=operation[5],
                DECISIVE=operation[6]
            ))

        return dict(status='success',
                    data=dict(operations=operations_data,
                              operations_num=operations_num,
                              ))

    except Exception as e:
        print(e)
        return dict(status='failure', data='Backend error: %s' % e)


# flask view for the proxy page
def proxy_page_view(sf, proxy):

    # test snowflake connection and reconnect if necessary
    try:
        if sf.is_closed():
            sf = sf_connect()
        if sf.is_closed():
            raise Exception('Reconnection failed')

    except Exception as e:
        print(e)
        return dict(status='failure', data='Database connection error')

    try:

        # SQL injection prevention
        if not(len(proxy) == 42 and proxy[:2] == '0x'):
            return render_template('unknown.html', object_name='proxy', object_value=proxy)

        proxy_data = sf.execute(f"""
            SELECT v.voter, '', v.stake, v.yay1, v.yay2, v.yay3, v.yay4, v.yay5, v.since, v.last_voting
            FROM {os.getenv("MCDGOV_DB", "mcd_public")}.public.current_voters v  
            WHERE lower(v.voter) = '{proxy}';
        """).fetchall()

        if len(proxy_data) != 1:
            return render_template('unknown.html', object_name='proxy', object_value=proxy)

        proxy_data = proxy_data[0]

        hot, cold = sf.execute(f"""
            select hot, cold
            from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.stg_proxies
            where proxy = '{proxy}';
        """).fetchone()

        last_update = sf.execute(f"""
            SELECT max(load_id)
            FROM {os.getenv("MCDGOV_DB", "mcd_public")}.internal.votes_scheduler
        """).fetchone()

        return render_template(
            'proxy.html',
            proxy=proxy_data[0],
            stake="{0:,.2f}".format(proxy_data[2]) if proxy_data[2] else '0.00',
            since=proxy_data[8].strftime("%Y-%m-%d") if proxy_data[8] else '',
            last_vote=proxy_data[9].strftime("%Y-%m-%d %H:%M:%S") if proxy_data[9] else '',
            refresh=last_update[0].__str__()[:19],
            hot=hot,
            cold=cold
        )

    except Exception as e:
        print(e)
        return render_template(
            'error.html',
            error_message=str(e)
        )
