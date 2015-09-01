var cy;

$(function(){ // on dom ready

  var query = location.search.substr(1);
  var namespace = null;
  var name = null;

  query.split("&").forEach(function(part) {
    var item = part.split("=");
    if(item[0] == "ns") {
      namespace = item[1];
    }
    else if(item[0] == "name") {
      name = item[1];
    }
  });

  $.getJSON(("viz/data/" + namespace + "/" + name), function(data) {
    // console.log(JSON.stringify(data))
    cy = cytoscape({
      container: document.getElementById('cy'),

      style: [{"selector":"core","style":{"selection-box-color":"#AAD8FF","selection-box-border-color":"#8BB0D0","selection-box-opacity":"0.5"}},{"selector":"node","style":{"width":"mapData(score, 0, 0.006769776522008331, 20, 60)","height":"mapData(score, 0, 0.006769776522008331, 20, 60)","content":"data(name)","font-size":"12px","text-valign":"center","text-halign":"center","background-color":"#555","text-outline-color":"#555","text-outline-width":"2px","color":"#fff","overlay-padding":"6px","z-index":"10"}},{"selector":"node[?attr]","style":{"shape":"rectangle","background-color":"#aaa","text-outline-color":"#aaa","width":"16px","height":"16px","font-size":"6px","z-index":"1"}},{"selector":"node[?query]","style":{"background-clip":"none","background-fit":"contain"}},{"selector":"node:selected","style":{"border-width":"6px","border-color":"#AAD8FF","border-opacity":"0.5","background-color":"#77828C","text-outline-color":"#77828C"}},{"selector":"edge","style":{"content": 'data(name)', "curve-style":"haystack","haystack-radius":"0.5","opacity":"0.8","line-color":"#bbb","width":"mapData(weight, 5, 1, 1, 8)","overlay-padding":"3px"}},{"selector":"node.unhighlighted","style":{"opacity":"0.2"}},{"selector":"edge.unhighlighted","style":{"opacity":"0.05"}},{"selector":".highlighted","style":{"z-index":"999999"}},{"selector":"node.highlighted","style":{"border-width":"6px","border-color":"#AAD8FF","border-opacity":"0.5","background-color":"#394855","text-outline-color":"#394855","shadow-blur":"12px","shadow-color":"#000","shadow-opacity":"0.8","shadow-offset-x":"0px","shadow-offset-y":"4px"}},{"selector":"edge.filtered","style":{"opacity":"0"}},{"selector":"edge[group=\"coexp\"]","style":{"line-color":"#d0b7d5"}},{"selector":"edge[group=\"coloc\"]","style":{"line-color":"#a0b3dc"}},{"selector":"edge[group=\"gi\"]","style":{"line-color":"#90e190"}},{"selector":"edge[group=\"path\"]","style":{"line-color":"#9bd8de"}},{"selector":"edge[group=\"pi\"]","style":{"line-color":"#eaa2a2"}},{"selector":"edge[group=\"predict\"]","style":{"line-color":"#f6c384"}},{"selector":"edge[group=\"spd\"]","style":{"line-color":"#dad4a2"}},{"selector":"edge[group=\"spd_attr\"]","style":{"line-color":"#D0D0D0"}},{"selector":"edge[group=\"reg\"]","style":{"line-color":"#D0D0D0"}},{"selector":"edge[group=\"reg_attr\"]","style":{"line-color":"#D0D0D0"}},{"selector":"edge[group=\"user\"]","style":{"line-color":"#f0ec86"}}],

      elements: data
    });

    var params = {
      name: 'cola',
      nodeSpacing: 10,
      edgeLengthVal: 200,
      animate: true,
      randomize: false,
      maxSimulationTime: 1500
    };
    var layout = makeLayout();
    var running = false;

    cy.on('layoutstart', function(){
      running = true;
    }).on('layoutstop', function(){
      running = false;
    });

    layout.run();

    var $config = $('#config');
    var $btnParam = $('<div class="param"></div>');
    $config.append( $btnParam );

    var sliders = [
      {
        label: 'Edge length',
        param: 'edgeLengthVal',
        min: 1,
        max: 400
      },

      {
        label: 'Node spacing',
        param: 'nodeSpacing',
        min: 1,
        max: 50
      }
    ];

    var buttons = [
      {
        label: '<i class="fa fa-random"></i>',
        layoutOpts: {
          randomize: true,
          flow: null
        }
      },

      {
        label: '<i class="fa fa-long-arrow-down"></i>',
        layoutOpts: {
          flow: { axis: 'y', minSeparation: 30 }
        }
      }
    ];

    sliders.forEach( makeSlider );

    buttons.forEach( makeButton );

    function makeLayout( opts ){
      params.randomize = false;
      params.edgeLength = function(e){ return params.edgeLengthVal; };

      for( var i in opts ){
        params[i] = opts[i];
      }

      return cy.makeLayout( params );
    }



    function makeSlider( opts ){
      var $input = $('<input></input>');
      var $param = $('<div class="param"></div>');

      $param.append('<span class="label label-default">'+ opts.label +'</span>');
      $param.append( $input );

      $config.append( $param );

      var p = $input.slider({
        min: opts.min,
        max: opts.max,
        value: params[ opts.param ]
      }).on('slide', _.throttle( function(){
        params[ opts.param ] = p.getValue();

        layout.stop();
        layout = makeLayout();
        layout.run();
      }, 16 ) ).data('slider');
    }

    function makeButton( opts ){
      var $button = $('<button class="btn btn-default">'+ opts.label +'</button>');

      $btnParam.append( $button );

      $button.on('click', function(){
        layout.stop();

        if( opts.fn ){ opts.fn(); }

        layout = makeLayout( opts.layoutOpts );
        layout.run();
      });
    }

    var id = 1000
    cy.on('select', 'node', function(e){
        var node = this;

        cy.add({
                    group: 'edges',
                    data: {
                      "source":node.data("id"),"target":"605846","weight":0.019493334,"group":"spd","networkId":1230,"networkGroupId":24,"intn":true,"rIntnId":id,"id":"e"+id}
                  });
                  id = id + 1
      });

    cy.nodes().forEach(function(n){
      var g = n.data('name');

      n.qtip({
        content: [
          {
            name: 'GeneCard',
            url: 'http://www.genecards.org/cgi-bin/carddisp.pl?gene=' + g
          },
          {
            name: 'UniProt search',
            url: 'http://www.uniprot.org/uniprot/?query='+ g +'&fil=organism%3A%22Homo+sapiens+%28Human%29+%5B9606%5D%22&sort=score'
          },
          {
            name: 'GeneMANIA',
            url: 'http://genemania.org/search/human/' + g
          }
        ].map(function( link ){
          return '<a target="_blank" href="' + link.url + '">' + link.name + '</a>';
        }).join('<br />\n'),
        position: {
          my: 'top center',
          at: 'bottom center'
        },
        style: {
          classes: 'qtip-bootstrap',
          tip: {
            width: 16,
            height: 8
          }
        }
      });
    });

  //  cy.on('mousemove','node', function(event){
  //                  var target = event.cyTarget;
  //                  var sourceName = target.data("source");
  //                  var targetName = target.data("target");
  //
  //                  var x=event.cyPosition.x;
  //                  var y=event.cyPosition.y;
  //
  //    var g = cy.nodes()[0].data('name');
  //
  //                          cy.nodes()[0].qtip({
  //                                                   content: [
  //                                                     {
  //                                                       name: 'GeneCard',
  //                                                       url: 'http://www.genecards.org/cgi-bin/carddisp.pl?gene=' + g
  //                                                     },
  //                                                     {
  //                                                       name: 'UniProt search',
  //                                                       url: 'http://www.uniprot.org/uniprot/?query='+ g +'&fil=organism%3A%22Homo+sapiens+%28Human%29+%5B9606%5D%22&sort=score'
  //                                                     },
  //                                                     {
  //                                                       name: 'GeneMANIA',
  //                                                       url: 'http://genemania.org/search/human/' + g
  //                                                     }
  //                                                   ].map(function( link ){
  //                                                     return '<a target="_blank" href="' + link.url + '">' + link.name + '</a>';
  //                                                   }).join('<br />\n'),
  //                                                   position: {
  //                                                     my: 'top center',
  //                                                     at: 'bottom center'
  //                                                   },
  //                                                   style: {
  //                                                     classes: 'qtip-bootstrap',
  //                                                     tip: {
  //                                                       width: 16,
  //                                                       height: 8
  //                                                     }
  //                                                   }
  //                                                 })
  //                  });

    $('#config-toggle').on('click', function(){
      $('#config').toggleClass('config-closed');
    });
  });
}); // on dom ready

$(function() {
  FastClick.attach( document.body );
});
