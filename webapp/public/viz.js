const {DeckGL, HexagonLayer} = deck;

const deckgl = new DeckGL({
    mapboxApiAccessToken: 'EXAMPLE_TOKEN_HERE',
    mapStyle: 'mapbox://styles/mapbox/dark-v9',
    longitude: -1.4157,
    latitude: 52.2324,
    zoom: 6,
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

    const hexagonLayer = new HexagonLayer({
        id: 'heat-map',
        colorRange: COLOR_RANGE,
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

d3.csv('https://raw.githubusercontent.com/uber-common/deck.gl-data/master/examples/3d-heatmap/heatmap-data.csv').then(res => {
    data = res.map(d => [Number(d.lng), Number(d.lat)]);
    renderLayer();
});