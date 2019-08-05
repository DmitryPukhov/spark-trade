# spark-trade

 <strong>Lambda architecture</strong> used:  batch layer for heavy analytics and speed layer for realtime streaming.<br/>
 <strong>Design</strong>:<br/>
 <ul>
 <li>1. Acquisition layer reads raw data to lake</li>
 <li>2. Messaging layer listens raw streams from price providers</li>
 
 <li>3. Ingestion layer listens messaging layer and transforms raw format to speed layer related</li>
 <li>4. Lambda layer contains of Batch and Speed layers
 <li>4.1 Batch layer reads raw data from lake and prepare batch views</li>
 <li>4.2 Speed layer listens ingestion layer and prepare speed views</li>
 </li>
 <li>5. Serving layer is a facade for data marts, calculated by batch and speed layers</li>
 <li>6. Datamarts are facades and combine data from speed and batch views</li>
 </ul>
