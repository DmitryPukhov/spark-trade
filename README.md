# spark-trade

 <strong>Lambda architecture</strong> used:  batch layer for heavy analytics and speed layer for realtime streaming.<br/>
 <strong>Design</strong>:<br/>
 1. Acquisition layer reads raw data to lake
 2. Messaging layer listens raw streams from price providers
 3. Ingestion layer listens messaging layer and transforms raw format to speed layer related
 4. Lambda layer contains of Batch and Speed layers
 4.1 Batch layer reads raw data from lake and prepare batch views
 4.2 Speed layer listens ingestion layer and prepare speed views
 5. Serving layer is a facade for data marts, calculated by batch and speed layers
 6. Datamarts are facades and combine data from speed and batch views
