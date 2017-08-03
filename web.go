package main

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"os/exec"
	"sort"
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

func webApply(m interface{}) error {
	t, err := template.New("web").Parse(webHTML)
	if err != nil {
		return err
	}
	f, err := ioutil.TempFile("", "web")
	if err != nil {
		return err
	}
	defer f.Close()
	if err := t.Execute(f, m); err != nil {
		return err
	}
	return exec.Command("open", f.Name()).Run()
}

type series struct {
	TargetAxisIndex int
	Color           string
	LineDashStyle   []int
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

	return webApply(m)
}

func web2(d []*testData) error {
	minConcurrency := d[0].runs[0].concurrency
	maxConcurrency := d[0].runs[len(d[0].runs)-1].concurrency
	for i := 1; i < len(d); i++ {
		if minConcurrency < d[i].runs[0].concurrency {
			minConcurrency = d[i].runs[0].concurrency
		}
		if n := len(d[i].runs); maxConcurrency > d[i].runs[n-1].concurrency {
			maxConcurrency = d[i].runs[n-1].concurrency
		}
	}

	have := func(d *testData, concurrency int) bool {
		i := sort.Search(len(d.runs), func(j int) bool {
			return d.runs[j].concurrency >= concurrency
		})
		return i < len(d.runs) && d.runs[i].concurrency == concurrency
	}

	get := func(d *testData, concurrency int) testRun {
		i := sort.Search(len(d.runs), func(j int) bool {
			return d.runs[j].concurrency >= concurrency
		})
		if i < len(d.runs) && d.runs[i].concurrency == concurrency {
			return *d.runs[i]
		}
		if i+1 >= len(d.runs) {
			return *d.runs[len(d.runs)-1]
		}
		if i < 0 {
			return *d.runs[0]
		}
		a := d.runs[i]
		b := d.runs[i+1]
		t := float64(concurrency-a.concurrency) / float64(b.concurrency-a.concurrency)
		return testRun{
			concurrency: concurrency,
			ops:         a.ops + int64(float64(b.ops-a.ops)*t),
			opsSec:      a.opsSec + float64(b.opsSec-a.opsSec)*t,
		}
	}

	data := []interface{}{
		[]interface{}{
			"concurrency",
			fmt.Sprintf("ops/sec (%s)", d[0].metadata.Bin),
			fmt.Sprintf("99%%-lat (%s)", d[0].metadata.Bin),
			fmt.Sprintf("ops/sec (%s)", d[1].metadata.Bin),
			fmt.Sprintf("99%%-lat (%s)", d[1].metadata.Bin),
		},
	}
	for i := minConcurrency; i <= maxConcurrency; i++ {
		if !have(d[0], i) && !have(d[1], i) {
			continue
		}
		r0 := get(d[0], i)
		r1 := get(d[1], i)
		data = append(data, []interface{}{
			i, r0.opsSec, r0.p99Lat, r1.opsSec, r1.p99Lat,
		})
	}

	m := map[string]interface{}{
		"data":  data,
		"haxis": "concurrency",
		"vaxes": []string{"ops/sec", "latency (ms)"},
		"series": []series{
			{0, "#ff0000", []int{}},
			{1, "#ff0000", []int{2, 2}},
			{0, "#0000ff", []int{}},
			{1, "#0000ff", []int{2, 2}},
		},
	}
	return webApply(m)
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
          legend: { position: 'top', alignment: 'center', textStyle: {fontSize: 12}, maxLines: 5 },
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
