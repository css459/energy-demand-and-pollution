## Lightweight Node.js wrapper app for large spatial data visualizations

#### Directions to run:

- `npm install`
- specify own `mapboxApiAccessToken` in `viz.js` or use the demo one provided for background map
- `node app.js`

App starts on `port 8080` and is easily configurable. Only dependency is `express`. MapBox token can be
requested at https://account.mapbox.com/access-tokens/. The visualization still works without a MapBox 
token, but the background map layer will not be rendered.

#### Visualization

The app visualizes a heatmap of lat, long data points representing the total "pollution badness". The data comes from the
main Spark application and is computed by taking the **L2 Norm** of the normalized **Criteria Gasses** features at each lat/long pair.
Please consult the main `README.md` of the project as well as the `Heatmap.scala` class for more information. 

Visualizations are done with [deck.gl], a powerful, open-source big data viz framework built on top of D3. The viz layers support large
amounts of rows but profit from the simple structure of the source `.csv`, only needing two columns for `lon` and `lat`. 

[deck.gl]:https://deck.gl/#/