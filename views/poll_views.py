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
from utils.polls import get_poll


def poll_data_view(sf, poll):

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
        if not poll.isnumeric():
            return dict(status='failure', data='Unknown poll')

        exists, title, options = get_poll(int(poll))

        if not exists:
            return dict(status='failure', data='Unknown poll')
        
        start, end = sf.execute(f"""
            select start_timestamp, end_timestamp
            from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.yays
            where type = 'poll' and code = '{poll}';
        """).fetchone()

        operations_query = f"""select v.timestamp, v.tx_hash, v.voter, v.operation, v.dapproval, v.option, '', v.proxy
                              from {os.getenv("MCDGOV_DB", "mcd_public")}.public.votes v  
                              where v.yay = '{poll}'
                              order by v.order_index desc, v.operation desc; """

        operations = sf.execute(operations_query).fetchall()

        check_if_closed = list()
        for operation in operations:
            if operation[3] == 'FINAL_CHOICE':
                check_if_closed.append(operation[3])

        # if poll is not closed, get voting power from snowflake staging
        voting_power_dict = dict()
        if not check_if_closed:

            voting_power_cache = sf.execute(f"""
                select voter, voting_power
                from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.voting_power;
            """).fetchall()

            if voting_power_cache:
                for power in voting_power_cache:
                    voting_power_dict[power[0]] = power[1]

        poll_operations = []
        if options:
            options = {key: dict(title=value, votes=0, stake=0) for key, value in options.items()}
        else:
            options = dict()
        options['Not valid'] = dict(title="Not valid", votes=0, stake=0)
        voters = set()
        approval = 0

        last_operations = dict()
        for operation in operations:
            if operation[2] not in last_operations and operation[0].__str__()[:19] <= end.__str__()[:19]:
                last_operations[operation[2]] = list(operation)
                # add voting power from cache
                if voting_power_dict:
                    if operation[2] in voting_power_dict:
                        last_operations[operation[2]][4] += voting_power_dict[operation[2]]

        # calculate options approval
        for operation in last_operations.values():

            votes = operation[5].split(',')
            voters.add(operation[2])
            approval += operation[4]

            if votes[0] in options:
                option = votes[0]
            else:
                option = "Not valid"
            options[option]['votes'] += 1
            options[option]['stake'] += operation[4] or 0

        # generate actions list
        for operation in operations:

            votes = operation[5].split(',')
            options_list = '<br>'.join([str(options[vote]['title']) if vote in options else 'Not valid' for vote in votes])

            if operation[7]:
                proxy = link(operation[7], f'/proxy/{operation[7]}', operation[7], new_window=False)
            else:
                proxy = ''
            
            if operation[3] == 'FINAL_CHOICE':
                op = operation[3]
            else:
                op = link(operation[3], 'https://ethtx.info/%s' % operation[1], operation[3],  new_window=True)

            if operation[3]:
                operation_row = [
                                    operation[0],
                                    link(operation[2], '/address/%s' % operation[2], operation[2]) if operation[2] else '',
                                    op,
                                    options_list,
                                    "{0:,.2f}".format(operation[4]),
                                    operation[4],
                                    proxy
                                ]
            else:

                operation_row = [
                                    operation[0],
                                    link(operation[2], '/address/%s' % operation[2], operation[2]) if operation[2] else '',
                                    '',
                                    options_list,
                                    "{0:,.2f}".format(operation[4]),
                                    operation[4],
                                    proxy
                                ]


            poll_operations.append(operation_row)

        operations_num = "{0:,d}".format(len(poll_operations))

        try:
            last_vote = max([operation[0] for operation in poll_operations])
        except:
            last_vote = ''

        approval = "{0:,.2f}".format(approval)

        num_voters = len(voters)

        not_valid = options.pop('Not valid')
        options_list = list(options.items())
        options_list.sort(key=lambda o: o[1]['stake'], reverse=True)
        options_list = [[key, options[key]['title'], "{0:,.2f}".format(o['stake']), "{0:,.0f}".format(o['votes'])] for key, o in options_list]
        options_list = [['Option', 'Option name', 'Stake (MKR)', 'Votes']] + options_list

        options_table = html_table(options_list, table_id='options', widths=['60px', None, '90px', '90px'], expose=[0], tooltip=False)

        # prepare output data
        operations_data = []
        for operation in poll_operations:
            operations_data.append(dict(
                TIME=operation[0].strftime("%Y-%m-%d %H:%M:%S"),
                ADDRESS=operation[1],
                PROXY=operation[6],
                OPERATION=operation[2],
                OPTION=operation[3],
                APPROVAL=operation[4]
            ))

        return dict(status='success',
                    data=dict(
                        poll_start=start.strftime("%Y-%m-%d %H:%M:%S"),
                        poll_end=end.strftime("%Y-%m-%d %H:%M:%S"),
                        last_vote=last_vote.strftime("%Y-%m-%d %H:%M:%S"),
                        num_voters=num_voters,
                        approval=approval,
                        options=options_table,
                        not_valid_num=not_valid['votes'],
                        not_valid_stake="{0:,.2f}".format(not_valid['stake']),
                        operations=operations_data,
                        operations_num=operations_num))

    except Exception as e:
        print(e)
        return dict(status='failure', data='Backend error: %s' % e)


# flask view for the poll page
def poll_page_view(sf, poll):

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
        if not poll.isnumeric():
            return render_template('unknown.html', object_name='poll', object_value=poll)

        exists, title, options = get_poll(int(poll))

        if not exists:
            return render_template('unknown.html', object_name='poll', object_value=poll)
        
        last_update = sf.execute(f"""
            SELECT max(load_id)
            FROM {os.getenv("MCDGOV_DB", "mcd_public")}.internal.votes_scheduler;
        """).fetchone()

        return render_template(
            'poll.html',
            yay=poll,
            title=title,
            refresh=last_update[0].__str__()[:19]
        )

    except Exception as e:
        print(e)
        return render_template(
            'error.html',
            error_message=str(e)
        )
