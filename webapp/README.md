## Lightweight Node.js wrapper app for visualizations

#### Directions to run:

- `npm install`
- specify `mapboxApiAccessToken` for background map
- `node app.js`

App starts on `port 8080` and is easily configurable. Only dependency is `Express`. MapBox token can be
requested at https://account.mapbox.com/access-tokens/. The visualization still works without a MapBox 
token, but the background map layer will not be rendered.

#### Visualization

So far the app visualizes a heatmap of lat, long from example data. It expects as input a URL to either a local or remote
`.csv` file with two columns `long` and `lat`. Visualizations are done with deck.gl.