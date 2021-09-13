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

def link(item, url, title=None, new_window=False):
    if title:
        title = 'title="%s"' % title
    else:
        title = ''
    return '<a %s%shref="%s">%s</a>' % (title, ' target="_blank" ' if new_window else ' ', url, item)


def poll_link(item, url, title=None, new_window=False, active=False):
    if title:
        title = f"""title='{title}'"""
    else:
        title = ''
    if active:
        active = """style='color: #1aab9b;' """
    else:
        active = ''
    return f"""<a {active}{title}{' target="_blank" ' if new_window else ' '}href="{url}">{item}</a>"""


def html_table(content, widths=None, table_class='simple-table', table_id='sorted-table', expose=None, tooltip=True):

    if len(content) > 1:

        html = "<table " + ("class='%s'" % table_class if table_class else '') + ("id='%s'" % table_id if table_id else '') + ">"
        html += "<thead><tr>"
        for i, column in enumerate(content[0]):
            if widths and len(widths) > i:
                width = widths[i]
            else:
                width = 'auto'
            html += "<th width='%s'>" % width + str(column) + "</th>"
        html += "</tr></thead>"

        html += "<tbody>"
        for row_num, content_row in enumerate(content[1:]):
            html += "<tr%s>" % ' class="row_expose"' if expose and row_num in expose else ''
            for i, content_item in enumerate(content_row):
                if tooltip and i == len(content_row) - 1:
                    html += '<td title="%s">' % str(content_item) + str(content_item) + "</td>"
                else:
                    html += "<td%s>" % (' class="row_expose"' if expose and row_num in expose else '') + str(content_item) + "</td>"
            html += "</tr>"
        html += "</tbody>"

        html += "</table>"
    else:
        html = ''

    return html
