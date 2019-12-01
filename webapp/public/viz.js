const {DeckGL, HexagonLayer} = deck;

const deckgl = new DeckGL({
    mapboxApiAccessToken: 'pk.eyJ1IjoiYWRvYnJvc2h5bnNreWkiLCJhIjoiY2sza3gwcGY1MHlwMzNtcGhud3YydTBiZyJ9.PBkZqsJXUX2zm2oaCyiHYQ',
    mapStyle: 'mapbox://styles/mapbox/dark-v9',
    longitude: -100.358887,
    latitude: 42.113014,
    zoom: 3,
    minZoom: 5,
    maxZoom: 15,
    pitch: 40.5
});

let data = null;
const OPTIONS = ['radius', 'coverage', 'upperPercentile'];
const COLOR_RANGE = [
    [1, 152, 189],
    [73, 227, 206],
    [216, 254, 181],
    [254, 237, 177],
    [254, 173, 84],
    [209, 55, 78]
];

OPTIONS.forEach(key => {
    document.getElementById(key).oninput = renderLayer;
});

function renderLayer () {
    const options = {};
    OPTIONS.forEach(key => {
        const value = document.getElementById(key).value;
        document.getElementById(key + '-value').innerHTML = value;
        options[key] = Number(value);
    });

    const hexagonLayer = new HeatmapLayer({
        id: 'heat-map',
        colorRange: COLOR_RANGE,
        threshold: 0.005,
        radiusPixels: 50,
        data,
        elevationRange: [0, 1000],
        elevationScale: 150,
        extruded: true,
        getPosition: d => d,
        opacity: 1,
        ...options
    });
    deckgl.setProps({
        layers: [hexagonLayer]
    });
}

d3.csv('heatmap-gasses-2019.csv').then(res => {
    data = res.map(d => [Number(d.lon), Number(d.lat)]);
    renderLayer();
});