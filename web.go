package main

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"os/exec"
	"sort"
)

func web(dirs []string) error {
	// TODO(peter): visualize the output of a single test run, showing
	// performance and latency over time.
	switch len(dirs) {
	case 0:
		return fmt.Errorf("no test directory specified")
	case 1, 2:
	default:
		return fmt.Errorf("too many test directories: %s", dirs)
	}

	var data []*testData
	for _, dir := range dirs {
		d, err := loadTestData(dir)
		if err != nil {
			return err
		}
		data = append(data, d)
	}

	if len(data) == 1 {
		return web1(data[0])
	}
	return web2(data[0], data[1])
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

func web2(d1, d2 *testData) error {
	minConcurrency := d1.runs[0].concurrency
	if c := d2.runs[0].concurrency; minConcurrency < c {
		minConcurrency = c
	}
	maxConcurrency := d1.runs[len(d1.runs)-1].concurrency
	if c := d2.runs[len(d2.runs)-1].concurrency; maxConcurrency > c {
		maxConcurrency = c
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
			elapsed:     a.elapsed + float64(b.elapsed-a.elapsed)*t,
			ops:         a.ops + int64(float64(b.ops-a.ops)*t),
			opsSec:      a.opsSec + float64(b.opsSec-a.opsSec)*t,
			avgLat:      a.avgLat + float64(b.avgLat-a.avgLat)*t,
			p50Lat:      a.p50Lat + float64(b.p50Lat-a.p50Lat)*t,
			p95Lat:      a.p95Lat + float64(b.p95Lat-a.p95Lat)*t,
			p99Lat:      a.p99Lat + float64(b.p99Lat-a.p99Lat)*t,
		}
	}

	data := []interface{}{
		[]interface{}{
			"concurrency",
			fmt.Sprintf("ops/sec (%s)", d1.metadata.Bin),
			fmt.Sprintf("99%%-lat (%s)", d1.metadata.Bin),
			fmt.Sprintf("ops/sec (%s)", d2.metadata.Bin),
			fmt.Sprintf("99%%-lat (%s)", d2.metadata.Bin),
		},
	}
	for i := minConcurrency; i <= maxConcurrency; i++ {
		if !have(d1, i) && !have(d2, i) {
			continue
		}
		r0 := get(d1, i)
		r1 := get(d2, i)
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
