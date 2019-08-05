# spark-trade

 <strong>Lambda architecture</strong> used:  batch layer for heavy analytics and speed layer for realtime streaming.<br/>
 <strong>Design</strong>:<br/>
 <ol>
 <li>Acquisition layer reads raw data to lake</li>
 <li>Messaging layer listens raw streams from price providers</li>
  <li>Ingestion layer listens messaging layer and transforms raw format to speed layer related</li>
 <li>Lambda layer contains of Batch and Speed layers
 <li>Batch layer reads raw data from lake and prepare batch views</li>
 <li>Speed layer listens ingestion layer and prepare speed views</li>
 <li>Serving layer is a facade for data marts, calculated by batch and speed layers</li>
 <li>Datamarts are facades and combine data from speed and batch views</li>
 </ol>
