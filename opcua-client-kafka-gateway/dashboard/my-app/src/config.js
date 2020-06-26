import * as firebase from 'firebase';
import 'firebase/firestore';

const settings = {timestampsInSnapshots:  true}

var firebaseConfig = {
    apiKey: "AIzaSyClwk1jSXo1OcrB4oJIWbVSwk40Lxar_CA",
    authDomain: "opcua-client-kafka-gateway.firebaseapp.com",
    databaseURL: "https://opcua-client-kafka-gateway.firebaseio.com",
    projectId: "opcua-client-kafka-gateway",
    storageBucket: "opcua-client-kafka-gateway.appspot.com",
    messagingSenderId: "726905065111",
    appId: "1:726905065111:web:77635c6228bf43c5b663d2",
    measurementId: "G-FH42FLCWKN"
  };

  firebase.initializeApp(firebaseConfig);
  firebase.firestore().settings(settings);

  export default firebase;