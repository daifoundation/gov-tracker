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

import json
import plotly
import plotly.graph_objs as go


def yay_graph(x, y, labels):

    data1 = [
            go.Scatter(
                x=x,
                y=y,
                name='Approval (MKR)',
                line={'color': '#1aab9b'},
                fill='tozeroy',
                mode="lines+markers",
                text=labels,
                yaxis="y"
            )
        ]

    layout1 = go.Layout(title={'text': "MKR approval in time (UTC)", 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top'},
                        plot_bgcolor='rgba(0,0,0,0)',
                        height=239, margin={"b": 20, "l": 20, "r": 10, "t": 45, "pad": 10},
                        xaxis={'gridcolor': '#F0F0F0'}, yaxis={'gridcolor': '#F0F0F0'},
                        hovermode="x unified", hoverlabel={'namelength': -1})

    figure1 = go.Figure(data=data1, layout=layout1)

    graph_json1 = json.dumps(figure1, cls=plotly.utils.PlotlyJSONEncoder)

    return graph_json1
