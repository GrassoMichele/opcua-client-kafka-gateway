import React from 'react';
import './App.css';
import firebase from './config';
import 'bootstrap/dist/css/bootstrap.min.css';
import {Card} from 'react-bootstrap';

class App extends React.Component{
  constructor(props){
    super(props);
    this.ref = firebase.firestore().collection("OpcUaNodes");
    this.unsubscribe = null;
    this.state = {
      records : []
    }
  }
  componentDidMount(){
    document.title = 'OPC UA Dashboard';
    this.unsubscribe = this.ref.onSnapshot(this.onCollectionUpdate);
  }
  onCollectionUpdate = (querySnapshot)=>{
    const records = [];
    querySnapshot.forEach((doc)=>{
      const {node, server, status, value, servertimestamp, sourcetimestamp} = doc.data();
      records.push({
        key: doc.id,
        doc,
        node,
        server,
        status,
        value,
        servertimestamp,
        sourcetimestamp
      });
    });
    this.setState({
      records
    });
  }

  render(){
    const cardStyles = {
      width: 'auto',
      height: 'auto',
      backgroundColor: 'white',
      margin:'auto',
      display:'block',
      marginTop:'60px',
      opacity: 0.5,
      paddingTop:'10px',
      paddingLeft: '20px',
      paddingRight: '20px',
      borderStyle: 'outset',
      borderLeft: '50px solid black',
      borderRadius: '10px'
    }
    return(
      <div>
        <Card style = {cardStyles}>
          <div class = "container">
            <div class = "panel panel-heading">
              <h3 class="panel-heading">Record Details</h3>
            </div>
          </div>
          <div class="panel-body">
            <table class="table table-stripe">
              <thead>
                <tr>
                  <th>Value</th>
                  <th>Node</th>
                  <th>Server</th>
                  <th>Source Timestamp</th>
                  <th>Server Timestamp</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {this.state.records.map(record =>
                <tr>
                  <td>{record.value}</td>
                  <td>{record.node}</td>
                  <td>{record.server}</td>
                  <td>{record.servertimestamp}</td>
                  <td>{record.sourcetimestamp}</td>
                  <td>{record.status}</td>
                </tr>
                )}
              </tbody>
            </table>
          </div>

        </Card>
      </div>
    )
  }
}
export default App;