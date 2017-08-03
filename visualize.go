package main

import (
	"fmt"
	"html/template"
	"os"
)

func visualize(dirs []string) error {
	var data []*testData
	for _, dir := range dirs {
		d, err := loadTestData(dir)
		if err != nil {
			return err
		}
		data = append(data, d)
	}

	switch len(data) {
	case 0:
		return fmt.Errorf("no test directory specified")
	case 1:
		return visualize1(data[0])
	case 2:
		return visualize2(data)
	default:
		return visualizeN(data)
	}
}

func visualize1(d *testData) error {
	data := []interface{}{
		[]interface{}{"concurrency", "ops/sec", "avg latency", "99%-tile latency"},
	}
	for _, r := range d.runs {
		data = append(data, []interface{}{
			r.concurrency, r.opsSec, r.avgLat, r.p99Lat,
		})
	}

	t, err := template.New("visualize").Parse(visualizeHTML)
	if err != nil {
		return err
	}
	m := map[string]interface{}{
		"data": data,
	}
	if err := t.Execute(os.Stdout, m); err != nil {
		return err
	}
	return nil
}

func visualize2(d []*testData) error {
	return fmt.Errorf("unimplemented")
}

func visualizeN(d []*testData) error {
	return fmt.Errorf("unimplemented")
}

const visualizeHTML = `<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['corechart']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          {{- range .data }}
          {{ . }},
          {{- end}}
        ]);

        var options = {
          legend: { position: 'top', alignment: 'center' },
          crosshair: { trigger: 'both', opacity: 0.35 },
          series: {
            0: {targetAxisIndex: 0, color: '#ff0000'},
            1: {targetAxisIndex: 1, color: '#ff0000', lineWidth: 1, lineDashStyle: [2, 2]},
            2: {targetAxisIndex: 1, color: '#ff0000', lineWidth: 1, lineDashStyle: [2, 2]},
          },
          vAxes: {
            0: {title: 'ops/sec'},
            1: {title: 'latency (ms)'},
          },
          hAxis: {
            title: 'concurrency',
          },
        };
        var chart = new google.visualization.LineChart(document.getElementById('chart'));
        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <div id="chart" style="width: 100%; height: 100%"></div>
  </body>
</html>
`
