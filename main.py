# This is a sample Python script.
import plotly.graph_objects as go
import json
import copy
import mysql.connector
from mysql.connector import errorcode
from operator import itemgetter
import requests
import dash
import dash_core_components as dcc
import dash_html_components as html
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

def getCoverage():
    output = {
        'suburb':set(),
        'postcode':set(),
        'acctRegion': {},
        'postCodeRef': {},
        'regionList': set()
    }
    try:
        cnx = mysql.connector.connect(user='development', password='flyme2themoon',
                                      host='drivers.letsportal.com.au',
                                      database='account')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = cnx.cursor()
        query = ("SELECT suburb,post_code,account_region FROM acct_suburb_postcode")
        cursor.execute(query)
        for (suburb,post_code,acct_region) in cursor:
            output['suburb'].add(suburb)
            output['postcode'].add(post_code)
            output['acctRegion'][suburb] = acct_region
            output['postCodeRef'][suburb] = post_code
            output['regionList'].add(acct_region)
        cnx.close()
    return output

def Extract(lst,idx):
    return list(map(itemgetter(idx), lst))

def getLocalBoundaries():
    getURL = 'https://data.gov.au/geoserver/vic-suburb-locality-boundaries-psma-administrative-boundaries/' \
             'wfs?request=GetFeature&typeName=ckan_af33dd8c_0534_4e18_9245_fc64440f742e&outputFormat=json'
    payload = {}
    headers = {}
    response = requests.request("GET", getURL, headers=headers, data=payload)
    output = json.loads(response.text)
    return output

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    app = dash.Dash()
    server = app.server
    jsIn = getLocalBoundaries()
    suburbDict = getCoverage()
    mapboxToken = 'pk.eyJ1IjoiczM3NTA5NTQiLCJhIjoiY2tiZjl3MnZnMGxsdzJxbjRpNDI3a2J3NyJ9.kOhuIXCnopGdLKBVIcvydg'
    # f = open('features.json')
    # jsIn = json.load(f)
    jsCopy = {}
    layerList = []
    colourList = ['red','yellow','blue','green','cyan','violet','black','white','orange']
    for region in suburbDict['regionList']:
        jsCopy[region] = copy.deepcopy(jsIn)
        jsCopy[region]['features'] = []
    centroid_lat = []
    centroid_lon = []
    hoverInfo = []
    for item in jsIn['features']:
        if item['properties']['vic_loca_2'] in suburbDict['suburb']:
            if (item['properties']['vic_loca_4'] is None) or (item['properties']['vic_loca_4'] in suburbDict['postcode']):
                jsCopy[suburbDict['acctRegion'][item['properties']['vic_loca_2']]]['features'].append(item)
                centroid_lon.append((max(Extract(item['geometry']['coordinates'][0][0], 0))
                                     + min(Extract(item['geometry']['coordinates'][0][0], 0)))/2)
                centroid_lat.append((max(Extract(item['geometry']['coordinates'][0][0], 1))
                                     + min(Extract(item['geometry']['coordinates'][0][0], 1))) / 2)
                hoverInfo.append(item['properties']['vic_loca_2']
                                 + ' '
                                 + suburbDict['postCodeRef'][item['properties']['vic_loca_2']]
                                 + ' : '
                                 + suburbDict['acctRegion'][item['properties']['vic_loca_2']]
                                 )
    colourCounter = 0
    for region in suburbDict['regionList']:
        layerList.append(
            {
                'source': jsCopy[region],
                'type': "fill",
                'below': "traces",
                'color': colourList[colourCounter],
                'opacity': 0.5
            }
        )
        colourCounter += 1

    fig = go.Figure(go.Scattermapbox(
        mode = "markers",
        lon = centroid_lon, lat = centroid_lat,
        text = hoverInfo,
        hoverinfo = 'text',
        marker = {
            'size': 10,
            'color': ["black"],
            'symbol': "information"
        }
    ))

    fig.update_layout(
        mapbox={
            'accesstoken': mapboxToken,
            'style': "basic",
            'center': {'lon': 144.95, 'lat': -37.81},
            # -37.814364596881546, 144.9597853404137
            'zoom': 12,
            'layers': layerList
        },
        margin={'l': 0, 'r': 0, 'b': 0, 't': 0})


    app.layout = html.Div([
        dcc.Graph(figure=fig)
    ])
    app.run_server(debug=True, use_reloader=False)
    # fig.write_html("file.html")
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
