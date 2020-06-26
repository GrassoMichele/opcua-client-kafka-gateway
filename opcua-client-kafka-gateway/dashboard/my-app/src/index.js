import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import * as serviceWorker from './serviceWorker';
import ShowRecord from './Components/ShowRecords.js';
import {BrowserRouter, Route} from 'react-router-dom';
import '../node_modules/bootstrap/dist/css/bootstrap.min.css'


ReactDOM.render(<BrowserRouter>
<Route exact path="/" component = {App}></Route>
<Route path="/show/:id" component = {ShowRecord}></Route>
</BrowserRouter>, document.getElementById('root'));

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
