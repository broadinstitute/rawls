var cy;

$(function(){ // on dom ready
  // fake data for use when running html file directly in the browser
  if(location.origin.toLowerCase().indexOf("file") > -1) {
    var data = [{ "grabbable": true, "data": { "id": "#20:0", "name": "Map-for: test", "clazz": "Map", "attributes": { } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#20:1", "name": "Map-for: subject_HCC1143", "clazz": "Map", "attributes": { "tumor_platform": "illumina", "ref_fasta": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta", "tumor_strip_unpaired": "TRUE", "ref_dict": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.dict", "tumor_bai": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad.bai", "tumor_sample_type": "Tumor", "ref_intervals": "gs://cancer-exome-pipeline-demo-data/panel_100_genes.interval_list", "ref_ann": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.ann", "normal_library": "library_name", "tumor_bam": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad.bam", "normal_fastq1": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad_1.fastq", "normal_strip_unpaired": "TRUE", "normal_sample_type": "Blood", "ref_sa": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.sa", "age": "45", "ref_amb": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.amb", "disease": "lung cancer", "normal_fastq2": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad_2.fastq", "tumor_fastq2": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad_2.fastq", "normal_platform": "illumina", "ref_bwt": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.bwt", "tumor_fastq1": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad_1.fastq", "tumor_library": "library_name", "normal_bam": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad.bam", "normal_bai": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad.bai", "gender": "male", "ref_pac": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.pac" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#13:0", "name": "Entity-subject_HCC1143", "clazz": "Entity", "attributes": { "name": "subject_HCC1143", "entityType": "participant" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#20:2", "name": "Map-for: HCC1143_BL", "clazz": "Map", "attributes": { "sample_type": "Blood", "ref_fasta": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta", "ref_dict": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.dict", "ref_intervals": "gs://cancer-exome-pipeline-demo-data/panel_100_genes.interval_list", "ref_ann": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.ann", "ref_sa": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.sa", "fastq2": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad_2.fastq", "ref_amb": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.amb", "fastq1": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad_1.fastq", "library": "library_name", "strip_unpaired": "TRUE", "sample": "HCC1143_BL", "ref_bwt": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.bwt", "platform": "illumina", "bai": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad.bai", "bam": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad.bam", "ref_pac": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.pac" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#13:1", "name": "Entity-HCC1143_BL", "clazz": "Entity", "attributes": { "entityType": "sample", "name": "HCC1143_BL" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#20:3", "name": "Map-for: HCC1143", "clazz": "Map", "attributes": { "sample_type": "Tumor", "ref_fasta": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta", "ref_dict": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.dict", "ref_intervals": "gs://cancer-exome-pipeline-demo-data/panel_100_genes.interval_list", "ref_ann": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.ann", "ref_sa": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.sa", "fastq2": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad_2.fastq", "ref_amb": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.amb", "fastq1": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad_1.fastq", "library": "library_name", "strip_unpaired": "TRUE", "sample": "HCC1143", "ref_bwt": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.bwt", "platform": "illumina", "bai": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad.bai", "bam": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad.bam", "ref_pac": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.64.pac" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#13:2", "name": "Entity-HCC1143", "clazz": "Entity", "attributes": { "entityType": "sample", "name": "HCC1143" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#20:4", "name": "Map-for: HCC1143_pair", "clazz": "Map", "attributes": { "ref_fasta": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta", "ref_dict": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.dict", "control_bai": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad.bai", "vcf_output_name": "mutations.vcf", "ref_intervals": "gs://cancer-exome-pipeline-demo-data/panel_100_genes.interval_list", "ref_ann": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.ann", "ref_fai": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.fai", "ref_sa": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.sa", "control_bam": "gs://cancer-exome-pipeline-demo-data/HCC1143.100_gene_250bp_pad.bam", "case_bai": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad.bai", "ref_amb": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.amb", "case_bam": "gs://cancer-exome-pipeline-demo-data/HCC1143_BL.100_gene_250bp_pad.bam", "ref_bwt": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.bwt", "ref_pac": "gs://cancer-exome-pipeline-demo-data/Homo_sapiens_assembly19.fasta.pac" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#13:3", "name": "Entity-HCC1143_pair", "clazz": "Entity", "attributes": { "entityType": "pair", "name": "HCC1143_pair" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "id": "#12:0", "name": "Workspace-test", "clazz": "Workspace", "attributes": { "name": "test", "createdBy": "abaumann@broadinstitute.org", "bucketName": "test-4c19a8df-5c6d-4c66-8595-8464e45c2ef6", "namespace": "broad-dsde-dev", "createdDate": "Tue Sep 01 17:12:42 EDT 2015" } }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "nodes" }, { "grabbable": true, "data": { "name": "OWN_attributes", "source": "#13:0", "clazz": "Edge", "attributes": { }, "id": "#13:0->#20:1", "target": "#20:1" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "REF_participant_id", "source": "#20:2", "clazz": "Edge", "attributes": { }, "id": "#20:2->#13:0", "target": "#13:0" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_attributes", "source": "#13:1", "clazz": "Edge", "attributes": { }, "id": "#13:1->#20:2", "target": "#20:2" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "REF_participant_id", "source": "#20:3", "clazz": "Edge", "attributes": { }, "id": "#20:3->#13:0", "target": "#13:0" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_attributes", "source": "#13:2", "clazz": "Edge", "attributes": { }, "id": "#13:2->#20:3", "target": "#20:3" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "REF_control_sample_id", "source": "#20:4", "clazz": "Edge", "attributes": { }, "id": "#20:4->#13:1", "target": "#13:1" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "REF_case_sample_id", "source": "#20:4", "clazz": "Edge", "attributes": { }, "id": "#20:4->#13:2", "target": "#13:2" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "REF_participant_id", "source": "#20:4", "clazz": "Edge", "attributes": { }, "id": "#20:4->#13:0", "target": "#13:0" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_attributes", "source": "#13:3", "clazz": "Edge", "attributes": { }, "id": "#13:3->#20:4", "target": "#20:4" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_attributes", "source": "#12:0", "clazz": "Edge", "attributes": { }, "id": "#12:0->#20:0", "target": "#20:0" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_participant", "source": "#12:0", "clazz": "Edge", "attributes": { }, "id": "#12:0->#13:0", "target": "#13:0" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_sample", "source": "#12:0", "clazz": "Edge", "attributes": { }, "id": "#12:0->#13:1", "target": "#13:1" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_sample", "source": "#12:0", "clazz": "Edge", "attributes": { }, "id": "#12:0->#13:2", "target": "#13:2" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }, { "grabbable": true, "data": { "name": "OWN_pair", "source": "#12:0", "clazz": "Edge", "attributes": { }, "id": "#12:0->#13:3", "target": "#13:3" }, "selected": false, "removed": false, "locked": false, "selectable": true, "grabbed": false, "group": "edges" }];
    loadViz(data);
  }
  else {
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
      loadViz(data);
    });
  }


  function loadViz(data) {
    // use to get test data to paste into the fake data section above
    //    console.log(JSON.stringify(data));

    var cy = cytoscape({
      container: document.getElementById('cy'),

      style: cytoscape.stylesheet()
        .selector('node')
          .css({
            'background-color': '#bbbbbb',
            "border-width":"2",
            "border-color":"#000",
            "border-opacity":"0.2",
            'content': 'data(name)',
            "text-valign":"center",
            "text-halign":"center"
          })
          .selector('node[clazz=\"Workspace\"]')
            .css({
              'background-color': '#FF0000',
            })
          .selector('node[clazz=\"Entity\"]')
            .css({
              'background-color': '#00FF00',
            })
          .selector('node[clazz=\"Map\"]')
            .css({
              'background-color': '#61bffc',
            })
        .selector('edge')
          .css({
            'target-arrow-shape': 'triangle',
            'width': 4,
            'line-color': '#ddd',
            'content': 'data(name)',
            'target-arrow-color': '#aaa',
            'edge-text-rotation': 'autorotate',
            "font-style": "italic",
            "font-size":"11px",
            "text-outline-color":"#555"
          })
        .selector('.highlighted')
          .css({
            'background-color': '#61bffc',
            'line-color': '#61bffc',
            'target-arrow-color': '#61bffc',
            'transition-property': 'background-color, line-color, target-arrow-color',
            'transition-duration': '0.5s'
          }),

      elements: data,
    });

    var params = {
      name: 'breadthfirst',
      directed: true,
      fit: true, // whether to fit the viewport to the graph
      padding: 10, // padding on fit
      circle: false, // put depths in concentric circles if true, put depths top down if false
      spacingFactor: 7, // positive spacing factor, larger => more space between nodes (N.B. n/a if causes overlap)
      boundingBox: undefined, // constrain layout bounds; { x1, y1, x2, y2 } or { x1, y1, w, h }
      avoidOverlap: true, // prevents node overlap, may overflow boundingBox if not enough space
      roots: undefined, // the roots of the trees
      maximalAdjustments: 0, // how many times to try to position the nodes in a maximal way (i.e. no backtracking)
      animate: true, // whether to transition the node positions
      animationDuration: 500, // duration of animation in ms if enabled
      ready: undefined, // callback on layoutready
      stop: undefined, // callback on layoutstop
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

    function makeLayout( opts ){
      return cy.makeLayout( params );
    }

    // select fn
    cy.on('select', 'node', function(e){
      var node = this;

    });

    cy.nodes().forEach(function(n){
      var g = n.data('attributes');

      n.qtip({
        content: [
          {
            name: JSON.stringify(g, "  ", 2)
          }
        ].map(function( link ){
          return '<pre><span class="inner-pre" style="font-size: 10px">'+link.name+'</span></pre>';
        }).join('<br />\n'),
        position: {
          my: 'top center',
          at: 'middle center'
        },
        style: {
          classes: 'ui-tooltip',
          tip: {
            width: 16,
            height: 30
          }
        }
      });
    });

    cy.on('mousemove','node', function(event){
                      var target = event.cyTarget;
                      var sourceName = target.data("source");
                      var targetName = target.data("target");

                      var x=event.cyPosition.x;
                      var y=event.cyPosition.y;
     });

      $('#config-toggle').on('click', function(){
        $('#config').toggleClass('config-closed');
      });
    }
}); // on dom ready

$(function() {
  FastClick.attach( document.body );
});
