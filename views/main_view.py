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
from threading import active_count
from flask import render_template
from datetime import datetime

from connectors.sf import sf_connect
from utils.polls import get_all_polls
from utils.utils import async_queries
from utils.tables import html_table, link, poll_link


def main_page_data(sf):

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

        voters_query = f"""select distinct v.voter, '', v.stake, v.yay1, v.yay2, v.yay3, v.yay4, v.yay5, v.since, v.last_voting 
                          from {os.getenv("MCDGOV_DB", "mcd_public")}.public.current_voters v  
                          order by v.stake desc; """

        yays_query = f"""select yay, option, first_voting, approval, voters, first_voting, type from
                        (select 'executive' as type, yay, null as option, round(sum(dapproval), 6) as approval, 
                                count(distinct voter) as voters, min(timestamp) as first_voting
                        from {os.getenv("MCDGOV_DB", "mcd_public")}.public.votes where operation not in ('CHOOSE', 'FINAL_CHOICE', 'RETRACT', 'CREATE_PROXY', 'BREAK_PROXY') group by yay
                        union
                        select 'poll' as type, yay, option, round(sum(dapproval), 6) as approval, 
                                count(distinct voter) as voters, min(timestamp) as first_voting
                        from {os.getenv("MCDGOV_DB", "mcd_public")}.public.votes where operation = 'CHOOSE' group by yay, option); """

        titles_query = f"""select code, title from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.yays; """

        hat_query = f"""select hat as hat from {os.getenv("MCDGOV_DB", "mcd_public")}.public.votes order by order_index desc limit 1; """

        active_polls_query = f"""select code from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.yays where type = 'poll' and end_timestamp > (select max(load_id) from {os.getenv("MCDGOV_DB", "mcd_public")}.internal.votes_scheduler); """

        all_queries = [
            dict(query=voters_query, id='voters'),
            dict(query=yays_query, id='yays'),
            dict(query=titles_query, id='titles'),
            dict(query=hat_query, id='hat'),
            dict(query=active_polls_query, id='active_polls')
        ]

        # snowflake data ingestion

        sf_responses = async_queries(sf, all_queries)
        voters = sf_responses['voters']
        yays = sf_responses['yays']
        titles = sf_responses['titles']
        hat = sf_responses['hat'][0][0]
        active_polls_repsonse = sf_responses['active_polls']

        active_polls = list()
        [active_polls.append(code[0]) for code in active_polls_repsonse]

        polls_metadata = get_all_polls()

        titles = {y[0]: y[1] for y in titles}
        titles['0x0000000000000000000000000000000000000000'] = 'Activate DSChief v1.2'

        staked = active = 0
        last_vote = None
        for v in voters:
            staked += v[2]
            if round(v[2], 6) > 0:
                active += 1
            if not last_vote or v[9] > last_vote:
                last_vote = v[9]

        voters_list = [[link(v[0], '/address/%s' % v[0], v[0]),
                        "{0:,.2f}".format(v[2] or 0),
                        '<br>'.join([link(titles.get(y, y), '/yay/%s' % y, titles.get(y, y)) for y in v[3:8] if y]),
                        v[8].date(), v[9] or ''] for v in voters]

        # prepare output data
        voters_data = []
        for voter in voters_list:
            voters_data.append(dict(
                VOTER=voter[0],
                STAKE=voter[1],
                CURRENT_VOTES=voter[2],
                SINCE=voter[3].strftime("%Y-%m-%d"),
                LAST=voter[4].strftime("%Y-%m-%d %H:%M:%S")
            ))

        executives = [y for y in yays if y[6] == 'executive']

        expose = [i for i in range(len(executives)) if executives[i][0] == hat]

        executives_list = [[link(y[0], '/yay/%s' % y[0]), y[5].date(), titles.get(y[0], y[0]),
                            "{0:,.2f}".format(y[3] or 0), y[4] or 0] for y in executives]
        executives_list = [['Spell', 'Since', 'Description', 'Approval<br>(MKR)', 'Voters<br>(#)']] + executives_list

        yays_table = html_table(executives_list, table_id='executives-table',
                                widths=['40px', '55px', None, '55px', '45px'], expose=expose, tooltip=False)

        polls_records = [y for y in yays if y[6] == 'poll']
        polls = dict()
        for poll in polls_records:
            if poll[0] not in polls:
                polls[poll[0]] = dict(votes=0, stake=0, max_stake=0, winning=None, since=poll[2])

            polls[poll[0]]['votes'] += poll[4]
            polls[poll[0]]['stake'] += poll[3]
            if not polls[poll[0]]['winning'] or poll[3] > polls[poll[0]]['max_stake']:
                polls[poll[0]]['winning'] = poll[1]
                polls[poll[0]]['max_stake'] = poll[3]

        polls = [[key, *value.values()] for key, value in polls.items()]
        polls.sort(key=lambda x: int(x[0]) if x[0].isnumeric() else 0, reverse=True)

        polls_list = [[link(p[0], '/poll/%s' % p[0]), p[5].date(), polls_metadata[p[0]][0],
                       polls_metadata[p[0]][1].get(p[4], 'Unknown'),
                       1 if p[0] in active_polls else 0] for p in polls if p[0] in polls_metadata]
        polls_list = [['Poll', 'Since', 'Description', 'Winning<br>option', 'Active']] + polls_list

        polls_table = html_table(polls_list, table_id='polls-table', widths=['40px', '55px', None, '130px'], tooltip=False)

        return dict(status='success',
                    data=dict(staked="{0:,.2f}".format(staked),
                              active="{0:,.0f}".format(active),
                              last_vote=last_vote.strftime("%Y-%m-%d %H:%M:%S"),
                              voters=voters_data,
                              voters_num=len(voters),
                              yays=yays_table,
                              yays_num=len(executives),
                              polls=polls_table,
                              polls_num=len(polls)))

    except Exception as e:
        print(e)
        return dict(status='failure', data='Backend error: %s' % e)


# flask view for the main page
def main_page_view(sf):

    # test snowflake connection and reconnect if necessary
    try:
        if sf.is_closed():
            sf = sf_connect()
        if sf.is_closed():
            raise Exception('Reconnection failed')

    except Exception as e:
        print(e)
        return dict(status='failure', data='Database connection error')

    last_update = sf.execute(f"""
        SELECT max(load_id)
        FROM {os.getenv("MCDGOV_DB", "mcd_public")}.internal.votes_scheduler;
    """).fetchone()

    try:

        return render_template(
            'main.html',
            refresh=last_update[0].__str__()[:19]
        )

    except Exception as e:
        print(e)
        return render_template(
            'error.html',
            error_message=str(e)
        )
