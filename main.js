//var static = require('node-static');
var OplogWatcher = require('./connector.js');
var request = require("request");
//var webfolder = new static.Server('./web');

var aggregator = require('./aggregator.js');
var dispatcher = require('./dispatcher.js');

// event handler for incoming data
/*var processData = function(document) {
	console.info(document);
	console.info("#" + aggregator.count());
	
	if ( document.cid == "CLEAR" ) {
		aggregator.reset();
	} else {
		aggregator.addData(document);
	}
	
	// TODO push to clients only every x milliseconds
	dispatcher.emit( aggregator.aggregate() );
};*/

// init HTTP demon
/*var httpd = require('http').createServer(function(req,res) {
	req.addListener('end', function() {
		webfolder.serve(req,res);
	}
	).resume();
}
);
httpd.listen(8080);*/

// init client dispatcher
//dispatcher.init(httpd, 'map_reduce_aggregates');


var processInsertData = function(document) {
	// add data to NEO4J
  runCypherQuery(
    'CREATE (somebody:Pais  {_nombre_pais: {_nombre_pais}, musica: {musica}, arte: {arte}, juegos: {juegos}, peliculas: {peliculas}, anime: {anime}, vida: {vida}, amanecer: {amanecer}, amor: {amor}, familia: {familia}, tv: {tv}, medio_ambiente: {medio_ambiente}, mechanotherapeutic: {mechanotherapeutic}, protectorate: {protectorate}, phytopaleontological: {phytopaleontological}, springlet: {springlet}, diectasis: {diectasis}, sconcheon: {sconcheon}, noncompetitive: {noncompetitive}, bellonion: {bellonion}, hami: {hami}, accusable: {accusable}, operatable: {operatable}, linitis: {linitis}, hoastman: {hoastman}, unlanguid: {unlanguid}, isokurtic: {isokurtic}, benchboard: {benchboard}, gastroplication: {gastroplication}, alumiferous: {alumiferous}, chapterful: {chapterful}, yeldrock: {yeldrock}, axopodia: {axopodia}, smokebox: {smokebox}, fireworky: {fireworky}, semideltaic: {semideltaic}, intimation: {intimation}, tarsioid: {tarsioid}, fotui: {fotui}, woft: {woft}, Mandingo: {Mandingo}, pretincture: {pretincture}, pamper: {pamper}, extraneously: {extraneously}, consternation: {consternation}, malleate: {malleate}, thisn: {thisn}, ribbony: {ribbony}, Lapland: {Lapland}, Monticulipora: {Monticulipora}, cetylene: {cetylene}, fie: {fie}, pluvious: {pluvious}, gunl: {gunl}, monsieur: {monsieur}, producal: {producal}, Limnophilidae: {Limnophilidae}, daitya: {daitya}, octahedral: {octahedral}, pegger: {pegger} } ) RETURN somebody', { 
      _nombre_pais: document['_id'],
      musica: document['value']['musica'],
      arte: document['value']['arte'], 
      juegos: document['value']['juegos'], 
      peliculas: document['value']['peliculas'], 
      anime: document['value']['anime'], 
      vida: document['value']['vida'], 
      amanecer: document['value']['amanecer'], 
      amor: document['value']['amor'], 
      familia: document['value']['familia'], 
      tv: document['value']['tv'], 
      medio_ambiente: document['value']['medio ambiente'], 
      mechanotherapeutic: document['value']['mechanotherapeutic'], 
      protectorate: document['value']['protectorate'], 
      phytopaleontological: document['value']['phytopaleontological'], 
      springlet: document['value']['springlet'], 
      diectasis: document['value']['diectasis'], 
      sconcheon: document['value']['sconcheon'], 
      noncompetitive: document['value']['noncompetitive'], 
      bellonion: document['value']['bellonion'], 
      hami: document['value']['hami'], 
      accusable: document['value']['accusable'], 
      operatable: document['value']['operatable'], 
      linitis: document['value']['linitis'], 
      hoastman: document['value']['hoastman'], 
      unlanguid: document['value']['unlanguid'], 
      isokurtic: document['value']['isokurtic'], 
      benchboard: document['value']['benchboard'], 
      gastroplication: document['value']['gastroplication'], 
      alumiferous: document['value']['alumiferous'], 
      chapterful: document['value']['chapterful'], 
      yeldrock: document['value']['yeldrock'], 
      axopodia: document['value']['axopodia'], 
      smokebox: document['value']['smokebox'], 
      fireworky: document['value']['fireworky'], 
      semideltaic: document['value']['semideltaic'], 
      intimation: document['value']['intimation'], 
      tarsioid: document['value']['tarsioid'], 
      fotui: document['value']['fotui'], 
      woft: document['value']['woft'], 
      Mandingo: document['value']['Mandingo'], 
      pretincture: document['value']['pretincture'], 
      pamper: document['value']['pamper'], 
      extraneously: document['value']['extraneously'], 
      consternation: document['value']['consternation'], 
      malleate: document['value']['malleate'], 
      thisn: document['value']['thisn'], 
      ribbony: document['value']['ribbony'], 
      Lapland: document['value']['Lapland'], 
      Monticulipora: document['value']['Monticulipora'], 
      cetylene: document['value']['cetylene'], 
      fie: document['value']['fie'], 
      pluvious: document['value']['pluvious'], 
      gunl: document['value']['gunl'], 
      monsieur: document['value']['monsieur'], 
      producal: document['value']['producal'], 
      Limnophilidae: document['value']['Limnophilidae'], 
      daitya: document['value']['daitya'], 
      octahedral: document['value']['octahedral'], 
      pegger: document['value']['pegger']
    }, function (err, resp) {
      if (err) {
        console.log(err);
      } else {
        console.log(resp);
      }
    }
  ); 
};

var processDropData = function(document) {
	/// drop data from NEO4J
	runCypherQuery(
    'MATCH (n) OPTIONAL MATCH (n)-[r]-() delete n,r', {
    }, function (err, resp) {
      if (err) {
        console.log(err);
      } else {
        console.log(resp);
      }
    }
  );
};



// init Neo4J listener
var neo_host = '192.168.56.1',
  neo_port = 7474;

var httpUrlForTransaction = 'http://' + neo_host + ':' + neo_port + '/db/data/transaction/commit';
function runCypherQuery(query, params, callback) {
  request.post({
      uri: httpUrlForTransaction,
      json: {statements: [{statement: query, parameters: params}]},
      headers: {
        'Authorization': 'Basic ' + new Buffer("neo4j:root").toString('base64')
      }
    },
    function (err, res, body) {
      callback(err, body);
    })
}

// init MongoDB listener
var oplog = new OplogWatcher({
  host:"192.168.56.23",
  map_reduce: true,
  ns: "test.tmp.mr",
  ns_drop: "test"
});
oplog.on('insert', function(data) {
	console.info("insert", data);
	processInsertData(data);
});
oplog.on('create', function(data) {
	console.info("drop: ",data);
	processDropData(data);
});