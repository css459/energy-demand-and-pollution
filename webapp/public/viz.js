const {DeckGL} = deck;

// storing data as globals for re-rendering
let heatmapData = null;
let heatmapScoreData = null;

const OPTIONS = ['radiusPixels', 'threshold'];

OPTIONS.forEach(key => {
    document.getElementById(key).oninput = renderHeatmapLayer;
});

function renderScoreHeatmapLayer () {
    const SCORE_HEATMAP_COLOR_RANGE = [
        [55,255,204],
        [217,240,163],
        [173,221,142],
        [120,198,121],
        [49,163,84],
        [0,104,55]
    ];

    const options = {};
    OPTIONS.forEach(key => {
        const value = document.getElementById(key).value;
        document.getElementById(key + '-value').innerHTML = value;
        options[key] = Number(value);
    });

    new DeckGL({
        mapboxApiAccessToken: 'pk.eyJ1IjoiYWRvYnJvc2h5bnNreWkiLCJhIjoiY2sza3gwcGY1MHlwMzNtcGhud3YydTBiZyJ9.PBkZqsJXUX2zm2oaCyiHYQ',
        mapStyle: 'mapbox://styles/mapbox/dark-v9',
        container: 'heatmap-scores',
        longitude: -100.358887,
        latitude: 42.113014,
        zoom: 3,
        minZoom: 5,
        maxZoom: 15,
        pitch: 40.5,
        layers: [ new HeatmapLayer({
            id: 'heat-map-scores',
            colorRange: SCORE_HEATMAP_COLOR_RANGE,
            threshold: 0.005,
            radiusPixels: 50,
            data: heatmapScoreData,
            elevationRange: [0, 1000],
            elevationScale: 150,
            extruded: true,
            getPosition: d => d.coords,
            getWeight: d => d.score,
            opacity: 1,
            ...options
        })]
    });
}

function renderHeatmapLayer () {
    const HEATMAP_COLOR_RANGE = [
        [1, 152, 189],
        [73, 227, 206],
        [216, 254, 181],
        [254, 237, 177],
        [254, 173, 84],
        [209, 55, 78]
    ];

    const options = {};
    OPTIONS.forEach(key => {
        const value = document.getElementById(key).value;
        document.getElementById(key + '-value').innerHTML = value;
        options[key] = Number(value);
    });

    new DeckGL({
        mapboxApiAccessToken: 'pk.eyJ1IjoiYWRvYnJvc2h5bnNreWkiLCJhIjoiY2sza3gwcGY1MHlwMzNtcGhud3YydTBiZyJ9.PBkZqsJXUX2zm2oaCyiHYQ',
        mapStyle: 'mapbox://styles/mapbox/dark-v9',
        container: 'heatmap',
        longitude: -100.358887,
        latitude: 42.113014,
        zoom: 3,
        minZoom: 5,
        maxZoom: 15,
        pitch: 40.5,
        layers: [ new HeatmapLayer({
            id: 'heat-map',
            colorRange: HEATMAP_COLOR_RANGE,
            threshold: 0.005,
            radiusPixels: 50,
            data: heatmapData,
            elevationRange: [0, 1000],
            elevationScale: 150,
            extruded: true,
            getPosition: d => d,
            opacity: 1,
            ...options
        })]
    });
}

d3.csv('heatmap-gasses-2019.csv').then(res => {
    heatmapData = res.map(d => [Number(d.lon), Number(d.lat)]);
    renderHeatmapLayer();
});

d3.csv('pollution-cost-scores-2019.csv').then(res => {
    heatmapScoreData = res.map(d => {
        return {
            coords: [Number(d.lon), Number(d.lat)],
            score: Number(d.score)
        }
    });
    renderScoreHeatmapLayer();
});