# This is a sample Python script demonstrating a typical use-case of
import plotly.graph_objects as go
import json
import copy
import mysql.connector
from mysql.connector import errorcode
from operator import itemgetter
import requests
import pandas as pd
# import dash
# from dash import dcc
# from dash import html
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def read_school_table(file_name):
    # read a tab separated file
    df = pd.read_csv(file_name, sep='\t')
    # cleanse the school column, by taking only the first part separated by a comma
    df['School'] = df['School'].apply(lambda x: x.split(', ')[0].upper())
    # convert df to a dictionary, with the value in School column being the key for each record
    return df.set_index('School').T.to_dict()

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
                                      host='drivers-dev.letsportal.com.au',
                                      database='account', ssl_ca = 'certs_azure_dev/root-ca.pem', ssl_cert = 'certs_azure_dev/client-cert.pem', ssl_key = 'certs_azure_dev/client-key.pem')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = cnx.cursor()
        query = ("SELECT suburb,post_code, account_region FROM acct_suburb_postcode_teamicare WHERE service_range IS TRUE")
        cursor.execute(query)
        for (suburb,post_code,acct_region) in cursor:
            output['suburb'].add(suburb)
            output['postcode'].add(post_code)
            output['acctRegion'][suburb] = acct_region
            output['postCodeRef'][suburb] = post_code
            output['regionList'].add(acct_region)
        cnx.close()
    return output

def getColourSingle(zone_name:str, colour:str):
    # 'YES' - '234851'
    return {zone_name: colour}

def getSchoolColourPlan():
    # hard code a dict with scores and colour hex codes. scores are only 96, 97, 98, 99 and 100
    return {
        100: '#00FF00',
        99: '#99FF99',
        98: '#FFFF00',
        97: '#FF9900',
        96: '#FF0000'
    }

def getColourPlan():
    output = {}
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
        query = ("SELECT zone_code,zone_colour FROM regions")
        cursor.execute(query)
        for (zone_code,zone_colour) in cursor:
            output[zone_code] = zone_colour
        cnx.close()
    return output

def getRateMatrix():
    output = {}
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
        query = ("SELECT service_type,base_pc_rate_l,base_pc_rate_m,base_pc_rate_s,base_pc_rate_xs FROM charge_matrix")
        cursor.execute(query)
        output['Parcel Type'] = ['Documents and X-small','Small','Medium','Large']
        output['Weight'] = ['An adult can hold >= 10 at once (<1kg)',
                            'An adult can hold >= 4 at once (<5kg)',
                            'An adult can hold >= 2 at once (<10kg)',
                            'An adult can hold >= 1 at once (<22kg)'
                            ]
        for (service_type,base_pc_rate_l,base_pc_rate_m,base_pc_rate_s,base_pc_rate_xs) in cursor:
            output[service_type+' Service (excl.GST)'] = [base_pc_rate_xs,base_pc_rate_s,base_pc_rate_m,base_pc_rate_l]
        cnx.close()
    return output

def getZoneInteration():
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
        query = ("SELECT * FROM region_interactions")
        cursor.execute(query)
        output = pd.DataFrame(cursor,columns = ['from_zone','from_colour','to_zone','to_colour','cost'])
        output = output.pivot(index = 'from_zone', columns = 'to_zone',values = 'cost').round(2)
        cnx.close()
    return output

def Extract(lst,idx):
    return list(map(itemgetter(idx), lst))

def getLocalBoundariesSaved():
    f = open('locality.json')
    jsIn = json.load(f)
    return jsIn

def getSchoolBoundariesSaved():
    f = open('Primary_Integrated_2024.geojson')
    jsIn = json.load(f)
    return jsIn

def getLocalBoundaries():
    getURL = 'https://data.gov.au/geoserver/vic-suburb-locality-boundaries-psma-administrative-boundaries/wfs?request=GetFeature&typeName=ckan_af33dd8c_0534_4e18_9245_fc64440f742e&outputFormat=json'
    payload = {}
    headers = {}
    response = requests.request("GET", getURL)
    output = json.loads(response.text)
    # save this to a local file called locality.json
    with open('locality.json', 'w') as f:
        json.dump(output, f)
    return output

def create_school_fig():
    jsIn = getSchoolBoundariesSaved()
    suburbDict = read_school_table('top_10pct_primary_schools.tsv')
    unique_school_names = set(suburbDict.keys())
    available_school_names = set([item['properties']['School_Name'].upper() for item in jsIn['features']])
    # find the names missing from available_school_names but present in unique_school_names
    missing_school_names = unique_school_names - available_school_names
    print(f"Missing school names: {missing_school_names}")
    mapboxToken = 'pk.eyJ1IjoiczM3NTA5NTQiLCJhIjoiY2tiZjl3MnZnMGxsdzJxbjRpNDI3a2J3NyJ9.kOhuIXCnopGdLKBVIcvydg'
    jsCopy = {}
    layerList = []
    # colourList = getColourPlan()
    colourList = getSchoolColourPlan()
    for score in colourList.keys():
        jsCopy[score] = copy.deepcopy(jsIn)
        jsCopy[score]['features'] = []
    centroid_lat = []
    centroid_lon = []
    hoverInfo = []
    for item in jsIn['features']:
        if item['properties']['School_Name'].upper() in unique_school_names:
            # find the first element in suburbDict where the value for key 'School' matches item['properties']['School_Name']
            matching_element = suburbDict[item['properties']['School_Name'].upper()]
            if matching_element['State Overall Score'] > 0:
                jsCopy[matching_element['State Overall Score']]['features'].append(item)
            centroid_lon.append((max(Extract(item['geometry']['coordinates'][0], 0))
                                + min(Extract(item['geometry']['coordinates'][0], 0))) / 2)
            centroid_lat.append((max(Extract(item['geometry']['coordinates'][0], 1))
                                + min(Extract(item['geometry']['coordinates'][0], 1))) / 2)
            if item['properties']['School_Name'].upper() != item['properties']['Campus_Name'].upper():
                hoverInfo.append(item['properties']['School_Name']
                                + ' '
                                + item['properties']['Campus_Name'] + '-' + item['properties']['Year_Level']
                                # choose to show the region or not
                                + ' : '
                                + str(matching_element['State Overall Score'])
                                )
            else:
                hoverInfo.append(item['properties']['School_Name']
                                + '-' + item['properties']['Year_Level']
                                # choose to show the region or not
                                + ' : '
                                + str(matching_element['State Overall Score'])
                                )
    for score in colourList.keys():
        layerList.append(
            {
                'source': jsCopy[score],
                'type': "fill",
                'below': "traces",
                'color': colourList[score],
                'fill': {'outlinecolor':colourList[score]},
                'opacity': 0.4
            }
        )
    
    fig = go.Figure(go.Scattermapbox(
        # choose between marker or text mode
        mode = "text",
        lon = centroid_lon, lat = centroid_lat,
        text = hoverInfo,
        # hoverinfo = 'text',
        # marker = {
        #     'size': 10,
        #     'color': ["black"],
        #     'symbol': "information"
        # }
    ))

    fig.update_layout(
        mapbox={
            'accesstoken': mapboxToken,
            'style': "streets",
            'center': {'lon': 145.02, 'lat': -37.94},
            # -37.814364596881546, 144.9597853404137
            # -37.91725010865954, 145.02890041628314
            'zoom': 9,
            'layers': layerList
        },
        margin={'l': 0, 'r': 0, 'b': 0, 't': 0}
    )
    fig.write_html("index.html")
    # fig.write_image("teamicare-mel-zone-map.jpg")
    # fig.show
    return fig

def createFig():
    jsIn = getLocalBoundariesSaved()
    suburbDict = getCoverage()
    mapboxToken = 'pk.eyJ1IjoiczM3NTA5NTQiLCJhIjoiY2tiZjl3MnZnMGxsdzJxbjRpNDI3a2J3NyJ9.kOhuIXCnopGdLKBVIcvydg'
    # f = open('features.json')
    # jsIn = json.load(f)
    jsCopy = {}
    layerList = []
    # colourList = getColourPlan()
    colourList = getColourSingle('YES', '#7BBFCC')
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
                # show the suburb info only for a subset
                if item['properties']['vic_loca_2'] in ('MELBOURNE', 'CRANBOURNE', 'FRANKSTON', 'PAKENHAM', 'CROYDON', 'RESERVOIR', 'ST ALBANS', 'HOPPERS CROSSING', 'WERRIBEE', 'POINT COOK'):
                    centroid_lon.append((max(Extract(item['geometry']['coordinates'][0][0], 0))
                                        + min(Extract(item['geometry']['coordinates'][0][0], 0)))/2)
                    centroid_lat.append((max(Extract(item['geometry']['coordinates'][0][0], 1))
                                        + min(Extract(item['geometry']['coordinates'][0][0], 1))) / 2)
                    hoverInfo.append(item['properties']['vic_loca_2']
                                    # + ' '
                                    # + suburbDict['postCodeRef'][item['properties']['vic_loca_2']]
                                    # choose to show the region or not
                                    # + ' : '
                                    # + suburbDict['acctRegion'][item['properties']['vic_loca_2']]
                                    )
    for region in suburbDict['regionList']:
        layerList.append(
            {
                'source': jsCopy[region],
                'type': "fill",
                'below': "traces",
                'color': colourList[region],
                'fill': {'outlinecolor':colourList[region]},
                'opacity': 0.7
            }
        )

    fig = go.Figure(go.Scattermapbox(
        # choose between marker or text mode
        mode = "text",
        lon = centroid_lon, lat = centroid_lat,
        text = hoverInfo,
        # hoverinfo = 'text',
        # marker = {
        #     'size': 10,
        #     'color': ["black"],
        #     'symbol': "information"
        # }
    ))

    fig.update_layout(
        mapbox={
            'accesstoken': mapboxToken,
            'style': "light",
            'center': {'lon': 145.02, 'lat': -37.94},
            # -37.814364596881546, 144.9597853404137
            # -37.91725010865954, 145.02890041628314
            'zoom': 9,
            'layers': layerList
        },
        margin={'l': 0, 'r': 0, 'b': 0, 't': 0}
    )
    fig.write_html("teamicare-mel-zone-map.html")
    # fig.write_image("teamicare-mel-zone-map.jpg")
    # fig.show
    return fig

def createTable():
    sourceDF = getZoneInteration()
    colourList = getColourPlan()
    columnColours = [colourList[i] for i in sourceDF.columns]
    rowColours = [colourList[i] for i in sourceDF.index]
    fig = go.Figure(data=[go.Table(
        header=dict(values=['X-ZONE (AUD)'] + list(sourceDF.columns),
                    fill_color=['#ebf6ff'] + columnColours,
                    align='center',
                    font=dict(size=10),
                    height=25),
        cells=dict(values=[sourceDF.index] + [sourceDF[i] for i in sourceDF.columns],
                   fill_color=[rowColours,'#ebf6ff'],
                   align='center',
                   format = ['','.1f'],
                   font=dict(size=10),
                   height=25))
    ])
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0)
    )
    fig.write_html("lp-mel-zone-table.html")

def createRateCard():
    sourceDF = pd.DataFrame.from_dict(getRateMatrix())
    headerColor = ['#25241C']
    rowColourList = ['white', 'lightgrey']
    rowColours = [rowColourList[i%2] for i in range(len(sourceDF.index))]
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(sourceDF.columns),
                    fill_color=headerColor,
                    align='center',
                    font=dict(color='white',size=10)),
        cells=dict(values=[sourceDF[i] for i in sourceDF.columns],
                   fill_color=[rowColours*len(sourceDF.index)],
                   align='center',
                   format = ['','','.1f'],
                   font=dict(size=10)))
    ])
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0)
    )
    fig.write_html("lp-mel-rate-matrix.html")
# Press the green button in the gutter to run the script.
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
# server = app.server
# figToShow = createFig()
# app.layout = html.Div([
#     dcc.Graph(figure=figToShow)
# ])
if __name__ == '__main__':
    # getLocalBoundariesSaved()
    # getCoverage()
    # createTable()
    # createFig()
    create_school_fig()
    # createRateCard()
    # app.run_server(debug=True)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
