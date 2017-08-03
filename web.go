package main

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"os/exec"
)

func web(dirs []string) error {
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
		return web1(data[0])
	case 2:
		return web2(data)
	default:
		return webN(data)
	}
}

func web1(d *testData) error {
	data := []interface{}{
		[]interface{}{"concurrency", "ops/sec", "avg latency", "99%-tile latency"},
	}
	for _, r := range d.runs {
		data = append(data, []interface{}{
			r.concurrency, r.opsSec, r.avgLat, r.p99Lat,
		})
	}

	t, err := template.New("web").Parse(webHTML)
	if err != nil {
		return err
	}

	type series struct {
		TargetAxisIndex int
		Color           string
		LineDashStyle   []int
	}
	m := map[string]interface{}{
		"data":  data,
		"haxis": "concurrency",
		"vaxes": []string{"ops/sec", "latency (ms)"},
		"series": []series{
			{0, "#ff0000", []int{}},
			{1, "#ff0000", []int{2, 2}},
			{1, "#ff0000", []int{4, 4}},
		},
	}

	f, err := ioutil.TempFile("", "web")
	if err != nil {
		return err
	}
	defer f.Close()
	// return t.Execute(os.Stdout, m)
	if err := t.Execute(f, m); err != nil {
		return err
	}
	return exec.Command("open", f.Name()).Run()
}

func web2(d []*testData) error {
	return fmt.Errorf("unimplemented")
}

func webN(d []*testData) error {
	return fmt.Errorf("unimplemented")
}

const webHTML = `<html>
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
            {{- range $i, $e := .series }}
            {{ $i }}: {targetAxisIndex: {{- $e.TargetAxisIndex }}, color: {{ $e.Color }}, lineDashStyle: {{ $e.LineDashStyle }}},
            {{- end }}
          },
          vAxes: {
            {{- range $i, $e := .vaxes }}
            {{ $i }}: {title: {{ $e }}},
            {{- end }}
          },
          hAxis: {
            title: {{ .haxis }},
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
