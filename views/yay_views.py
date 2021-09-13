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
from datetime import datetime

from connectors.sf import sf_connect
from utils.tables import html_table, link

from graphs.yay_graph import yay_graph


def yay_data_view(sf, yay):

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
        if not(len(yay) == 42 and yay[:2] == '0x'):
            return dict(status='failure', data='Unknown yay')

        operations_query = f"""select v.timestamp, v.tx_hash, v.voter, v.operation, v.dapproval, v.decisive, '', v.hat, v.order_index
                              from {os.getenv("MCDGOV_DB", "mcd_public")}.public.votes v  
                              where v.yay = '{yay}'
                              order by v.order_index, v.operation; """

        operations = sf.execute(operations_query).fetchall()

        yay_operations = []
        approval = 0
        x = []
        y = []
        labels = []
        voters = dict()

        for operation in operations:

            approval += operation[4]
            if operation[2] not in voters:
                voters[operation[2]] = [operation[6] or '', 0, 0]

            voters[operation[2]][2] += operation[4] or 0
            if voters[operation[2]][2] > voters[operation[2]][1]:
                voters[operation[2]][1] = voters[operation[2]][2]

            operation_row = [operation[0],
                             link(operation[2], '/address/%s' % operation[2], operation[2]) if operation[2] else '',
                             link(operation[3], 'https://ethtx.info/%s' % operation[1], '%s transaction' % operation[3], new_window=True) if operation[3] else '',
                             "{0:,.2f}".format(operation[4] or 0), 'YES' if operation[5] else '', "{0:,.2f}".format(approval),
                             'YES' if operation[7] == yay else 'NO',
                             operation[8]]
            yay_operations.append(operation_row)

            x.append(operation[0])
            y.append(approval)
            labels.append(operation[3])

        operations_num = "{0:,d}".format(len(yay_operations))

        since = min([operation[0].date() for operation in operations])
        try:
            last_vote = max([operation[0] for operation in operations])
        except:
            last_vote = ''

        approval = "{0:,.2f}".format(sum([operation[4] for operation in operations]))

        num_voters = len(voters)
        voters = list(voters.items())
        voters.sort(key=lambda _x: _x[1][1], reverse=True)

        top_voters_list = [[link(v[0], '/address/%s' % v[0]),
                            "{0:,.2f}".format(v[1][1])] for v in voters[:9] if v[1][1]]
        top_voters_list = [['Address', 'Top stake']] + top_voters_list
        top_voters_table = html_table(top_voters_list, table_id='voters', widths=[None, '110px'], tooltip=False)

        plot = yay_graph(x, y, labels)

        # prepare output data
        operations_data = []
        for operation in yay_operations:
            operations_data.append(dict(
                TIME=operation[0].strftime("%Y-%m-%d %H:%M:%S") + ' ' + str(operation[7]),
                ADDRESS=operation[1],
                OPERATION=operation[2],
                VOTE=operation[3],
                DECISIVE=operation[4],
                APPROVAL=operation[5],
                HAT=operation[6]
            ))

        return dict(status='success',
                    data=dict(since=since.strftime("%Y-%m-%d"),
                              last_vote=last_vote.strftime("%Y-%m-%d %H:%M:%S"),
                              num_voters=num_voters,
                              approval=approval,
                              top_voters=top_voters_table,
                              operations=operations_data,
                              operations_num=operations_num,
                              plot=plot))

    except Exception as e:
        print(e)
        return dict(status='failure', data='Backend error: %s' % e)


# flask view for the yay page
def yay_page_view(sf, yay):

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
        if not (len(yay) == 42 and yay[:2] == '0x'):
            return render_template('unknown.html', object_name='yay', object_value=yay)

        yay = yay.lower()

        titles_query = f"""select code, title from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.yays where code = '{yay}'; """
        titles = sf.execute(titles_query).fetchone()

        if titles:
            title = titles[1]
        elif yay == '0x0000000000000000000000000000000000000000':
            title = 'Activate DSChief v1.2'
        else:
            title = yay
        
        last_update = sf.execute(f"""
            SELECT max(load_id)
            FROM {os.getenv("MCDGOV_DB", "mcd_public")}.internal.votes_scheduler
        """).fetchone()

        return render_template(
            'yay.html',
            yay=yay,
            title=title,
            refresh=last_update[0].__str__()[:19]
        )

    except Exception as e:
        print(e)
        return render_template(
            'error.html',
            error_message=str(e)
        )
