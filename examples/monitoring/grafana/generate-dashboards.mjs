// Generates the dagron Grafana dashboards in ./dashboards (every one except
// dagron-overview.json, which is maintained by hand).
//
//   node generate-dashboards.mjs
//
// The dashboards are plain Grafana JSON; this file exists so that 200 panels
// share one definition of a selector, a colour and a legend instead of 200.
// Change a panel here, re-run, and commit both this file and the JSON.
// See docs/METRICS.md for what each metric means.
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const outDir = join(dirname(fileURLToPath(import.meta.url)), "dashboards");
// The classic dashboard schema version of the Grafana release these are tested
// on (13.2.3). Grafana migrates older versions forward on load.
const schemaVersion = 42;

const DS = { type: "prometheus", uid: "${datasource}" };
const SEL = 'namespace=~"$namespace", job=~"$job", instance=~"$instance"';
const WF = `${SEL}, workflow=~"$workflow"`;
// The last_run gauges are per engine: each holds the last run that engine
// finalized. This keeps, per workflow, only the series from the engine whose
// last run is the newest, so an older run on another engine cannot stand in.
const LAST_TS = `scheduler_workflow_last_run_finished_timestamp_seconds{${WF}}`;
const newest = (metric) => `(${metric}{${WF}} and (${LAST_TS} == on (workflow) group_left max by (workflow) (${LAST_TS})))`;
const RI = "$__rate_interval";
// For counts per interval (bars, heatmap cells) the window must equal the step,
// or neighbouring points count the same event more than once. Those panels set
// a one-minute minimum interval and use this.
const STEP = "$__interval";

let nextId = 1;
let y = 0;

const target = (expr, legendFormat, extra = {}) => ({
  datasource: DS, editorMode: "code", expr, legendFormat, range: true, ...extra,
});
const instant = (expr, legendFormat, extra = {}) => target(expr, legendFormat, { instant: true, range: false, ...extra });
const tableQ = (expr) => instant(expr, "", { format: "table" });
const withRefs = (targets) => targets.map((t, i) => ({ ...t, refId: String.fromCharCode(65 + i) }));

function row(title, collapsed = false) {
  const p = { type: "row", id: nextId++, title, collapsed, gridPos: { h: 1, w: 24, x: 0, y }, panels: [] };
  y += 1;
  return p;
}
function line(h, panels) {
  let x = 0;
  const out = panels.map(([w, p]) => {
    const placed = { ...p, id: nextId++, gridPos: { h, w, x, y } };
    x += w;
    return placed;
  });
  y += h;
  return out;
}
const thresholds = (steps) => ({ mode: "absolute", steps: steps.map(([color, value]) => ({ color, value })) });
const fixed = (name, color) => ({ matcher: { id: "byName", options: name }, properties: [{ id: "color", value: { mode: "fixed", fixedColor: color } }] });
const STATUS_COLORS = [
  fixed("succeeded", "green"), fixed("failed", "red"), fixed("running", "blue"), fixed("pending", "yellow"),
  fixed("ready", "orange"), fixed("cancelled", "purple"), fixed("awaiting_approval", "super-light-blue"), fixed("skipped", "text"),
];

function stat(title, description, targets, { unit = "short", decimals, steps = [["green", null]], colorMode = "value", graphMode = "area", mappings = [], calc = "lastNotNull", noValue, textMode = "auto" } = {}) {
  return {
    type: "stat", title, description, datasource: DS,
    fieldConfig: {
      defaults: { unit, ...(decimals === undefined ? {} : { decimals }), ...(noValue ? { noValue } : {}), mappings, color: { mode: "thresholds" }, thresholds: thresholds(steps) },
      overrides: [],
    },
    options: {
      colorMode, graphMode, justifyMode: "auto", orientation: "auto", textMode,
      wideLayout: true, showPercentChange: false,
      reduceOptions: { calcs: [calc], fields: "", values: false },
    },
    targets: withRefs(targets),
  };
}

function timeseries(title, description, targets, { unit = "short", stack = false, fill = 10, legendCalcs = ["lastNotNull", "max"], legendPlacement = "bottom", min, max, softMax, decimals, overrides = [], bars = false, steps, points = false, perInterval = bars } = {}) {
  if (bars && softMax === undefined) softMax = 1;
  return {
    type: "timeseries", title, description, datasource: DS,
    ...(perInterval ? { interval: "1m" } : {}),
    fieldConfig: {
      defaults: {
        unit,
        ...(min === undefined ? {} : { min }), ...(max === undefined ? {} : { max }),
        ...(decimals === undefined ? {} : { decimals }),
        color: { mode: "palette-classic" },
        custom: {
          drawStyle: bars ? "bars" : points ? "points" : "line", lineInterpolation: "linear", lineWidth: 1,
          fillOpacity: bars ? 70 : fill, gradientMode: "none", showPoints: points ? "always" : "never", pointSize: 5,
          spanNulls: false, axisPlacement: "auto", axisBorderShow: false, axisCenteredZero: false,
          axisColorMode: "text", axisLabel: "", barAlignment: 0,
          ...(softMax === undefined ? {} : { axisSoftMax: softMax }),
          scaleDistribution: { type: "linear" },
          stacking: { group: "A", mode: stack ? "normal" : "none" },
          thresholdsStyle: { mode: steps ? "line" : "off" },
          hideFrom: { legend: false, tooltip: false, viz: false },
        },
        thresholds: thresholds(steps || [["green", null]]),
        mappings: [],
      },
      overrides,
    },
    options: {
      legend: { displayMode: "table", placement: legendPlacement, showLegend: true, calcs: legendCalcs },
      tooltip: { mode: "multi", sort: "desc", hideZeros: false },
    },
    targets: withRefs(targets),
  };
}

function heatmap(title, description, metric, { unit = "s" } = {}) {
  return {
    type: "heatmap", title, description, datasource: DS,
    fieldConfig: {
      defaults: { custom: { scaleDistribution: { type: "linear" }, hideFrom: { legend: false, tooltip: false, viz: false } } },
      overrides: [],
    },
    options: {
      calculate: false, cellGap: 1,
      color: { mode: "scheme", scheme: "Oranges", fill: "dark-orange", exponent: 0.5, reverse: false, scale: "exponential", steps: 64 },
      exemplars: { color: "rgba(255,0,255,0.7)" },
      filterValues: { le: 1e-9 },
      legend: { show: true },
      rowsFrame: { layout: "auto" },
      showValue: "never",
      tooltip: { mode: "single", showColorScale: false, yHistogram: true },
      yAxis: { axisPlacement: "left", reverse: false, unit },
    },
    interval: "1m",
    targets: withRefs([target(`sum by (le) (increase(${metric}_bucket{${SEL}}[${STEP}]))`, "{{le}}", { format: "heatmap" })]),
  };
}

function stateTimeline(title, description, targets, mappings, steps) {
  return {
    type: "state-timeline", title, description, datasource: DS,
    fieldConfig: {
      defaults: {
        // Fixed, not thresholds: in threshold mode the bars are labelled with the
        // threshold range ("1+") instead of the mapped text. Mappings carry the colour.
        color: { mode: "fixed", fixedColor: "text" }, mappings, thresholds: thresholds(steps),
        custom: { fillOpacity: 80, lineWidth: 0, insertNulls: false, spanNulls: false, axisPlacement: "auto", hideFrom: { legend: false, tooltip: false, viz: false } },
      },
      overrides: [],
    },
    options: {
      alignValue: "left", mergeValues: true, rowHeight: 0.9, showValue: "auto",
      legend: { displayMode: "list", placement: "bottom", showLegend: false },
      tooltip: { mode: "single", sort: "none", hideZeros: false },
    },
    targets: withRefs(targets),
  };
}

function barGauge(title, description, targets, { unit = "short", decimals = 0, color = "continuous-YlRd" } = {}) {
  return {
    type: "bargauge", title, description, datasource: DS,
    fieldConfig: {
      defaults: { unit, decimals, min: 0, color: { mode: color }, mappings: [], thresholds: thresholds([["green", null]]) },
      overrides: [],
    },
    options: {
      displayMode: "gradient", orientation: "horizontal", showUnfilled: true, valueMode: "color",
      namePlacement: "auto", sizing: "auto", minVizWidth: 8, minVizHeight: 16, maxVizHeight: 300,
      legend: { displayMode: "list", placement: "bottom", showLegend: false, calcs: [] },
      reduceOptions: { calcs: ["lastNotNull"], fields: "", values: false },
    },
    targets: withRefs(targets),
  };
}

function pie(title, description, targets, overrides = []) {
  return {
    type: "piechart", title, description, datasource: DS,
    fieldConfig: {
      defaults: { unit: "short", decimals: 0, color: { mode: "palette-classic" }, mappings: [], custom: { hideFrom: { legend: false, tooltip: false, viz: false } } },
      overrides,
    },
    options: {
      pieType: "donut", displayLabels: ["value"], sort: "desc",
      legend: { displayMode: "table", placement: "right", showLegend: true, values: ["value", "percent"] },
      reduceOptions: { calcs: ["lastNotNull"], fields: "", values: false },
      tooltip: { mode: "single", sort: "none", hideZeros: false },
    },
    targets: withRefs(targets),
  };
}

// A table joined on `key` from one instant query per column. `columns` is
// [[header, expr, {unit, decimals, steps, cell}]...]; the key column is named `keyHeader`.
function table(title, description, keys, columns, { sortBy, keyWidth = 200 } = {}) {
  const refs = columns.map((_, i) => String.fromCharCode(65 + i));
  const rename = { ...Object.fromEntries(keys.map(([label, header]) => [label, header])) };
  const exclude = { Time: true };
  columns.forEach(([header], i) => {
    rename[`Value #${refs[i]}`] = header;
    if (i > 0) exclude[`Time ${i + 1}`] = true;
  });
  return {
    type: "table", title, description, datasource: DS,
    fieldConfig: {
      defaults: { custom: { align: "auto", cellOptions: { type: "auto" }, inspect: false, filterable: true }, mappings: [], thresholds: thresholds([["green", null]]), color: { mode: "thresholds" } },
      overrides: [...keys.map(([, header]) => ({ matcher: { id: "byName", options: header }, properties: keyWidth ? [{ id: "custom.minWidth", value: keyWidth }] : [] })), ...columns.map(([header, , o = {}]) => ({
        matcher: { id: "byName", options: header },
        properties: [
          { id: "unit", value: o.unit || "short" },
          ...(o.decimals === undefined ? [] : [{ id: "decimals", value: o.decimals }]),
          ...(o.steps ? [{ id: "thresholds", value: thresholds(o.steps) }, { id: "custom.cellOptions", value: { type: "color-text" } }] : []),
          ...(o.gauge ? [{ id: "custom.cellOptions", value: { type: "gauge", mode: "gradient", valueDisplayMode: "text" } }, { id: "min", value: 0 }, { id: "max", value: o.gauge }] : []),
        ],
      }))],
    },
    options: { cellHeight: "sm", showHeader: true, footer: { show: false, reducer: ["sum"], countRows: false, fields: "" }, ...(sortBy ? { sortBy: [{ displayName: sortBy, desc: true }] } : {}) },
    targets: columns.map(([, expr], i) => ({ ...tableQ(expr), refId: refs[i] })),
    transformations: [
      { id: "merge", options: {} },
      { id: "filterFieldsByName", options: { include: { names: [...keys.map(([l]) => l), ...refs.map((r) => `Value #${r}`)] } } },
      { id: "organize", options: { excludeByName: {}, indexByName: Object.fromEntries([...keys.map(([l]) => l), ...refs.map((r) => `Value #${r}`)].map((n, i) => [n, i])), renameByName: rename } },
    ],
  };
}

const quantile = (q, metric, range = RI, sel = SEL) => `histogram_quantile(${q}, sum by (le) (rate(${metric}_bucket{${sel}}[${range}])))`;
const quantiles = (metric) => [0.5, 0.95, 0.99].map((q) => target(quantile(q, metric), `p${q * 100}`));
const rate = (metric) => `sum(rate(${metric}{${SEL}}[${RI}]))`;
const incRange = (metric) => `sum(increase(${metric}{${SEL}}[$__range]))`;
const incStep = (metric, by = "") => `sum${by ? ` by (${by})` : ""} (increase(${metric}{${SEL}}[${STEP}]))`;
// Datastore gauges are the same on every engine of one installation, so they are
// de-duplicated with `max` per namespace before anything is summed.
const ds = (metric, labels, extra = "", sel = SEL) => `max by (namespace${labels ? ", " + labels : ""}) (${metric}{${sel}${extra ? ", " + extra : ""}})`;

const queryVar = (name, label, query, { multi = true } = {}) => ({
  name, label, type: "query", datasource: DS, definition: query,
  query: { qryType: 1, query, refId: "PrometheusVariableQueryEditor-VariableQuery" },
  current: {}, includeAll: true, allValue: ".*", multi, options: [], refresh: 2, regex: "", sort: 1,
});
const WORKFLOW_VAR = queryVar("workflow", "Workflow", `label_values(scheduler_workflow_recent_runs{${SEL}}, workflow)`);

function dashboard({ uid, title, description, panels, extraVars = [], time = "now-1h" }) {
  return {
    annotations: {
      list: [{
        builtIn: 1, datasource: { type: "grafana", uid: "-- Grafana --" }, enable: true, hide: true,
        iconColor: "rgba(0, 211, 255, 1)", name: "Annotations & Alerts", type: "dashboard",
      }],
    },
    description, editable: true, fiscalYearStartMonth: 0, graphTooltip: 1,
    links: [{
      asDropdown: true, icon: "external link", includeVars: false, keepTime: true,
      tags: ["dagron"], targetBlank: false, title: "dagron dashboards", type: "dashboards", url: "",
    }],
    panels, preload: false, refresh: "30s", schemaVersion, tags: ["dagron"],
    templating: {
      list: [
        { name: "datasource", label: "Data source", type: "datasource", query: "prometheus", current: {}, includeAll: false, multi: false, options: [], refresh: 1, regex: "" },
        queryVar("namespace", "Namespace", "label_values(scheduler_uptime_seconds, namespace)"),
        queryVar("job", "Job", 'label_values(scheduler_uptime_seconds{namespace=~"$namespace"}, job)'),
        queryVar("instance", "Engine", 'label_values(scheduler_uptime_seconds{namespace=~"$namespace", job=~"$job"}, instance)'),
        ...extraVars,
      ],
    },
    time: { from: time, to: "now" }, timepicker: {}, timezone: "browser", title, uid, version: 1,
  };
}

function emit(file, spec) {
  writeFileSync(join(outDir, file), JSON.stringify(dashboard(spec), null, 2) + "\n");
  nextId = 1;
  y = 0;
}

const lat = (warn, crit) => [["green", null], ["yellow", warn], ["red", crit]];
const onOff = (off, on) => [{ type: "value", options: { 0: { text: off, color: "green", index: 0 }, 1: { text: on, color: "red", index: 1 } } }];
const clockMap = [{ type: "value", options: {
  0: { text: "synced", color: "green", index: 0 }, 1: { text: "drifted", color: "red", index: 1 }, 2: { text: "unknown", color: "yellow", index: 2 },
} }];

const recent = (status, by) => `sum by (${by}) (${ds("scheduler_workflow_recent_runs", "workflow, status", status ? `status=~"${status}"` : "")})`;
const recentTotal = (status) => `sum(${ds("scheduler_workflow_recent_runs", "workflow, status", status ? `status=~"${status}"` : "")})`;

// ── 1. Workflows by namespace ───────────────────────────────────────────────
{
  const anyBy = (by) => `sum by (${by}) (${ds("scheduler_workflow_recent_runs", "workflow, status")})`;
  const env = (status) => `sum by (environment) (${ds("scheduler_environment_recent_runs", "environment, status", status ? `status=~"${status}"` : "")})`;
  const panels = [
    ...line(4, [
      [4, stat("Namespaces", "Namespaces with at least one engine reporting.", [target(`count(count by (namespace) (scheduler_uptime_seconds{${SEL}}))`, "namespaces")], { decimals: 0, graphMode: "none" })],
      [4, stat("Engines", "Engines that answered their last scrape.", [target(`count(scheduler_uptime_seconds{${SEL}})`, "engines")], { decimals: 0, graphMode: "none", steps: [["red", null], ["green", 1]] })],
      [4, stat("Workflows run in 24 h", "Distinct workflows with a run created in the last 24 hours.", [target(`count(${anyBy("namespace, workflow")})`, "workflows")], { decimals: 0, graphMode: "none", noValue: "0" })],
      [4, stat("Runs in 24 h", "Runs created in the last 24 hours, in every status.", [target(recentTotal(""), "runs")], { decimals: 0, noValue: "0" })],
      [4, stat("In flight", "Runs that are pending or running now.", [target(`sum(${ds("scheduler_runs", "status", 'status=~"pending|running"')})`, "runs")], { decimals: 0, noValue: "0", steps: [["blue", null]] })],
      [4, stat("Failed in 24 h", "Runs created in the last 24 hours that ended failed.", [target(recentTotal("failed"), "failed")], { decimals: 0, noValue: "0", steps: [["green", null], ["red", 1]] })],
    ]),
    row("Namespaces"),
    ...line(6, [
      [24, table("Namespace summary", "One row per namespace. The namespace is the label Prometheus puts on the engine's scrape target: in Kubernetes the pod's namespace, elsewhere whatever the scrape config sets.",
        [["namespace", "Namespace"]],
        [
          ["Engines", `count by (namespace) (scheduler_uptime_seconds{${SEL}})`, { decimals: 0 }],
          ["Workflows (24 h)", `count by (namespace) (${anyBy("namespace, workflow")})`, { decimals: 0 }],
          ["Runs (24 h)", anyBy("namespace"), { decimals: 0 }],
          ["Failed (24 h)", recent("failed", "namespace"), { decimals: 0, steps: [["green", null], ["red", 1]] }],
          ["In flight", `sum by (namespace) (${ds("scheduler_runs", "status", 'status=~"pending|running"')})`, { decimals: 0 }],
          ["Queue depth", ds("scheduler_queue_depth", ""), { decimals: 0 }],
          ["Dead letters", ds("scheduler_dead_letters", ""), { decimals: 0, steps: [["green", null], ["red", 1]] }],
        ], { keyWidth: 0 })],
    ]),
    ...line(8, [
      [8, timeseries("Runs in flight by namespace", "Pending and running runs in each namespace.",
        [target(`sum by (namespace) (${ds("scheduler_runs", "status", 'status=~"pending|running"')})`, "{{namespace}}")], { decimals: 0, stack: true, fill: 30, min: 0 })],
      [8, timeseries("Runs created per second by namespace", "Run creation rate of the engines in each namespace.",
        [target(`sum by (namespace) (rate(scheduler_runs_created_total{${SEL}}[${RI}]))`, "{{namespace}}")], { unit: "ops", stack: true, fill: 30 })],
      [8, timeseries("Tasks finished per second by namespace", "Succeeded and failed tasks per second in each namespace.",
        [target(`sum by (namespace) (rate(scheduler_tasks_succeeded_total{${SEL}}[${RI}]))`, "{{namespace}} succeeded"),
          target(`sum by (namespace) (rate(scheduler_tasks_failed_total{${SEL}}[${RI}]))`, "{{namespace}} failed")], { unit: "ops" })],
    ]),
    row("Environments"),
    ...line(8, [
      [8, pie("Runs in 24 h by environment", "Runs created in the last 24 hours, by the environment each run named. Runs that named none are shown as (none).",
        [instant(env(""), "{{environment}}")])],
      [8, timeseries("Runs in flight by environment", "Pending and running runs created in the last 24 hours, by environment.",
        [target(env("pending|running"), "{{environment}}")], { decimals: 0, stack: true, fill: 30, min: 0 })],
      [8, barGauge("Failed in 24 h by environment", "Runs created in the last 24 hours that ended failed, by environment.",
        [instant(`sort_desc(${env("failed")})`, "{{environment}}")])],
    ]),
    row("Workflows"),
    ...line(10, [
      [24, table("Workflows by namespace", "Every workflow with a run in the last 24 hours. Only the 50 busiest workflows of an installation are named; the rest are summed as \"other\".",
        [["namespace", "Namespace"], ["workflow", "Workflow"]],
        [
          ["Runs (24 h)", anyBy("namespace, workflow"), { decimals: 0 }],
          ["Succeeded", recent("succeeded", "namespace, workflow"), { decimals: 0 }],
          ["Failed", recent("failed", "namespace, workflow"), { decimals: 0, steps: [["green", null], ["red", 1]] }],
          ["Running", recent("running", "namespace, workflow"), { decimals: 0 }],
          ["Tasks in progress", `sum by (namespace, workflow) (${ds("scheduler_workflow_active_tasks", "workflow, status")})`, { decimals: 0 }],
        ], { sortBy: "Runs (24 h)" })],
    ]),
  ];
  emit("dagron-namespaces.json", {
    uid: "dagron-namespaces", title: "dagron — workflows by namespace",
    description: "Which namespaces and environments are running what: engines, workflows, runs and failures per namespace, and the same by named environment.",
    panels, time: "now-3h",
  });
}

// ── 2. Workflow statistics ──────────────────────────────────────────────────
{
  const R = (status, by = "workflow") => `sum by (${by}) (${ds("scheduler_workflow_recent_runs", "workflow, status", status ? `status=~"${status}"` : "", WF)})`;
  const Rt = (status) => `sum(${ds("scheduler_workflow_recent_runs", "workflow, status", status ? `status=~"${status}"` : "", WF)})`;
  const finished = (status) => `sum by (workflow) (increase(scheduler_workflow_run_duration_seconds_count{${WF}${status ? `, status="${status}"` : ""}}[${STEP}]))`;
  // The engine emits no series for a status with no runs, so "nothing
  // succeeded" has to be turned into a zero or the ratio vanishes.
  const ratio24 = `(${Rt("succeeded")} or vector(0)) / ${Rt("succeeded|failed")}`;
  const panels = [
    ...line(4, [
      [4, stat("Success ratio (24 h)", "Succeeded over succeeded plus failed, for runs created in the last 24 hours. Cancelled and unfinished runs are in neither count.",
        [target(ratio24, "success")], { unit: "percentunit", decimals: 1, steps: [["red", null], ["yellow", 0.9], ["green", 0.99]], noValue: "no finished runs" })],
      [4, stat("Runs (24 h)", "Runs created in the last 24 hours.", [target(Rt(""), "runs")], { decimals: 0, noValue: "0" })],
      [4, stat("Succeeded (24 h)", "Runs created in the last 24 hours that succeeded.", [target(Rt("succeeded"), "succeeded")], { decimals: 0, noValue: "0" })],
      [4, stat("Failed (24 h)", "Runs created in the last 24 hours that failed.", [target(Rt("failed"), "failed")], { decimals: 0, noValue: "0", steps: [["green", null], ["red", 1]] })],
      [4, stat("Cancelled (24 h)", "Runs created in the last 24 hours that were cancelled.", [target(Rt("cancelled"), "cancelled")], { decimals: 0, noValue: "0", steps: [["text", null]] })],
      [4, stat("Running now", "Runs created in the last 24 hours that are pending or running.", [target(Rt("pending|running"), "running")], { decimals: 0, noValue: "0", steps: [["blue", null]] })],
    ]),
    row("Per workflow"),
    ...line(10, [
      [24, table("Workflow statistics", "One row per workflow, for runs created in the last 24 hours. Mean duration and last run come from the engines selected, since they started.",
        [["workflow", "Workflow"]],
        [
          ["Runs (24 h)", R(""), { decimals: 0 }],
          ["Succeeded", R("succeeded"), { decimals: 0 }],
          ["Failed", R("failed"), { decimals: 0, steps: [["green", null], ["red", 1]] }],
          ["Cancelled", R("cancelled"), { decimals: 0 }],
          ["Running", R("pending|running"), { decimals: 0 }],
          ["Success ratio", `(${R("succeeded")} or ${R("succeeded|failed")} * 0) / ${R("succeeded|failed")}`, { unit: "percentunit", decimals: 1, gauge: 1 }],
          ["Mean duration", `sum by (workflow) (scheduler_workflow_run_duration_seconds_sum{${WF}}) / sum by (workflow) (scheduler_workflow_run_duration_seconds_count{${WF}})`, { unit: "s", decimals: 1 }],
          ["Last run took", `max by (workflow) (${newest("scheduler_workflow_last_run_duration_seconds")})`, { unit: "s", decimals: 1 }],
          ["Last run ended", `time() - max by (workflow) (scheduler_workflow_last_run_finished_timestamp_seconds{${WF}})`, { unit: "s", decimals: 0 }],
        ], { sortBy: "Runs (24 h)" })],
    ]),
    ...line(8, [
      [12, timeseries("Runs finished by workflow", "Runs the selected engines finalized in each interval, by workflow.",
        [target(finished(""), "{{workflow}}")], { decimals: 0, stack: true, bars: true, min: 0, legendCalcs: ["sum"] })],
      [12, timeseries("Failed runs by workflow", "Runs that ended failed in each interval, by workflow.",
        [target(`${finished("failed")} > 0`, "{{workflow}}")], { decimals: 0, stack: true, bars: true, min: 0, legendCalcs: ["sum"] })],
    ]),
    ...line(8, [
      [12, stateTimeline("Last run outcome", "Whether each workflow's most recently finished run succeeded. A workflow that turns red and stays red is broken, not flaky.",
        [target(`min by (workflow) (${newest("scheduler_workflow_last_run_success")})`, "{{workflow}}")],
        [{ type: "value", options: { 0: { text: "failed", color: "red", index: 0 }, 1: { text: "succeeded", color: "green", index: 1 } } }], [["red", null], ["green", 1]])],
      [12, timeseries("Runs by status (24 h window)", "Runs created in the last 24 hours, by their current status.",
        [target(R("", "status"), "{{status}}")], { decimals: 0, stack: true, fill: 30, min: 0, overrides: STATUS_COLORS, legendCalcs: ["lastNotNull"] })],
    ]),
    row("What starts runs"),
    ...line(8, [
      [8, timeseries("Runs created per second", "Every run the selected engines created, whatever started it: the API, a schedule, a dataset trigger, a backfill or an ingest source.",
        [target(`sum by (instance) (rate(scheduler_runs_created_total{${SEL}}[${RI}]))`, "{{instance}}")], { unit: "ops", stack: true, fill: 30 })],
      [8, timeseries("Schedule decisions", "Fires a when: gate skipped, and schedules a stopStrategy expression switched off.",
        [target(incStep("scheduler_schedule_gated_total"), "fire skipped by when:"), target(incStep("scheduler_schedules_stopped_total"), "schedule stopped")], { decimals: 0, min: 0, bars: true })],
      [8, timeseries("Datasets", "Updates recorded (a produces: task succeeding, or an external event) and the runs those updates started through on_datasets:.",
        [target(rate("scheduler_dataset_updates_total"), "updates recorded"), target(rate("scheduler_dataset_fires_total"), "runs fired")], { unit: "ops" })],
    ]),
    (() => {
      const r = row("Catch-up and self-healing (feature build only)", true);
      r.panels = [
        ...line(4, [
          [8, stat("Overdue schedules", "Catch-up schedules with a missed fire still outstanding.", [target(`max(scheduler_overdue_schedules{${SEL}})`, "overdue")], { decimals: 0, graphMode: "none", noValue: "not emitted by this build", steps: [["green", null], ["yellow", 1]] })],
          [8, stat("Largest schedule lag", "Age of the oldest outstanding missed fire across all catch-up schedules.", [target(`max(scheduler_schedule_lag_seconds{${SEL}})`, "lag")], { unit: "s", noValue: "not emitted by this build", steps: [["green", null], ["yellow", 300], ["red", 3600]] })],
          [8, stat("Stalled runs", "Runs still running past the stall SLA.", [target(`max(scheduler_incomplete_runs{${SEL}})`, "stalled")], { decimals: 0, graphMode: "none", noValue: "not emitted by this build", steps: [["green", null], ["red", 1]] })],
        ]),
        ...line(8, [
          [12, timeseries("Schedule lag", "Largest catch-up lag across schedules.", [target(`max(scheduler_schedule_lag_seconds{${SEL}})`, "largest lag")], { unit: "s", fill: 0, min: 0 })],
          [12, timeseries("Runs made by catch-up and auto-rerun", "Runs materialized for missed fires, and failed runs re-armed from their failure frontier.",
            [target(incStep("scheduler_catchup_runs_total"), "catch-up runs"), target(incStep("scheduler_auto_reruns_total"), "auto reruns")], { decimals: 0, min: 0, bars: true })],
        ]),
      ];
      return r;
    })(),
  ];
  emit("dagron-workflow-statistics.json", {
    uid: "dagron-workflow-statistics", title: "dagron — workflow statistics",
    description: "How each workflow is doing: runs, outcomes and success ratio over the last 24 hours, the last run's result, and what is starting runs.",
    panels, extraVars: [WORKFLOW_VAR], time: "now-3h",
  });
}

// ── 3. Run time ─────────────────────────────────────────────────────────────
{
  const RD = "scheduler_run_duration_seconds";
  const mean = (range, status) => `sum by (workflow) (increase(scheduler_workflow_run_duration_seconds_sum{${WF}${status ? `, status="${status}"` : ""}}[${range}])) / sum by (workflow) (increase(scheduler_workflow_run_duration_seconds_count{${WF}${status ? `, status="${status}"` : ""}}[${range}]))`;
  const panels = [
    ...line(4, [
      [4, stat("Run time p50", "Median wall time of the runs finished in the selected time range, across all workflows.", [target(quantile(0.5, RD, "$__range"), "p50")], { unit: "s", graphMode: "none", noValue: "no finished runs" })],
      [4, stat("Run time p95", "95th percentile run wall time in the selected time range.", [target(quantile(0.95, RD, "$__range"), "p95")], { unit: "s", graphMode: "none", noValue: "no finished runs", steps: lat(600, 1800) })],
      [4, stat("Mean run time", "Mean wall time of the runs finished in the selected time range.",
        [target(`sum(increase(${RD}_sum{${SEL}}[$__range])) / sum(increase(${RD}_count{${SEL}}[$__range]))`, "mean")], { unit: "s", graphMode: "none", noValue: "no finished runs" })],
      [4, stat("Runs finished", "Runs the selected engines finalized in the selected time range.", [target(incRange(`${RD}_count`), "runs")], { decimals: 0, graphMode: "none" })],
      [4, stat("Slowest last run", "The longest most-recent run among the selected workflows.", [target(`max(${newest("scheduler_workflow_last_run_duration_seconds")})`, "slowest")], { unit: "s", graphMode: "none", noValue: "no finished runs", steps: lat(600, 1800) })],
      [4, stat("Queue wait p95", "95th percentile time a ready task waits before a worker takes it. Time in this number is queueing, not work.", [target(quantile(0.95, "scheduler_dispatch_latency_seconds", "$__range"), "p95")], { unit: "s", graphMode: "none", noValue: "no dispatches", steps: lat(5, 30) })],
    ]),
    row("All workflows"),
    ...line(9, [
      [12, timeseries("Run time", "Wall time from a run being created to reaching a terminal state: queueing, every task, and any wait at an approval gate.", quantiles(RD), { unit: "s", fill: 0 })],
      [12, heatmap("Run time distribution", "Runs finished per duration bucket in each interval. Buckets run from 1 second to 4 hours.", RD)],
    ]),
    row("Per workflow"),
    ...line(9, [
      [12, timeseries("Last run time by workflow", "The wall time of each workflow's most recent run, sampled at every scrape. A step up is a run that got slower.",
        [target(`max by (workflow) (${newest("scheduler_workflow_last_run_duration_seconds")})`, "{{workflow}}")], { unit: "s", fill: 0, min: 0, legendCalcs: ["lastNotNull", "mean", "max"] })],
      [12, barGauge("Mean run time by workflow", "Mean wall time of the runs finished in the selected time range, slowest first.",
        [instant(`sort_desc(${mean("$__range", "")})`, "{{workflow}}")], { unit: "s", decimals: 1, color: "continuous-BlYlRd" })],
    ]),
    ...line(9, [
      [12, timeseries("Mean run time by workflow", "Mean wall time of the runs finished in each interval.",
        [target(mean(STEP, ""), "{{workflow}}")], { unit: "s", fill: 0, min: 0, points: true, perInterval: true, legendCalcs: ["mean", "max"] })],
      [12, timeseries("Mean run time: succeeded against failed", "A failed run that takes as long as a successful one failed at the end; one that is much shorter failed early.",
        [target(`sum(increase(scheduler_workflow_run_duration_seconds_sum{${WF}, status="succeeded"}[${STEP}])) / sum(increase(scheduler_workflow_run_duration_seconds_count{${WF}, status="succeeded"}[${STEP}]))`, "succeeded"),
          target(`sum(increase(scheduler_workflow_run_duration_seconds_sum{${WF}, status="failed"}[${STEP}])) / sum(increase(scheduler_workflow_run_duration_seconds_count{${WF}, status="failed"}[${STEP}]))`, "failed")],
        { unit: "s", fill: 0, min: 0, points: true, perInterval: true, overrides: STATUS_COLORS, legendCalcs: ["mean", "max"] })],
    ]),
    ...line(8, [
      [24, table("Run time by workflow", "Totals since the selected engines started.",
        [["workflow", "Workflow"]],
        [
          ["Runs finished", `sum by (workflow) (scheduler_workflow_run_duration_seconds_count{${WF}})`, { decimals: 0 }],
          ["Mean run time", `sum by (workflow) (scheduler_workflow_run_duration_seconds_sum{${WF}}) / sum by (workflow) (scheduler_workflow_run_duration_seconds_count{${WF}})`, { unit: "s", decimals: 1 }],
          ["Mean when succeeded", `sum by (workflow) (scheduler_workflow_run_duration_seconds_sum{${WF}, status="succeeded"}) / sum by (workflow) (scheduler_workflow_run_duration_seconds_count{${WF}, status="succeeded"}) > 0`, { unit: "s", decimals: 1 }],
          ["Mean when failed", `sum by (workflow) (scheduler_workflow_run_duration_seconds_sum{${WF}, status="failed"}) / sum by (workflow) (scheduler_workflow_run_duration_seconds_count{${WF}, status="failed"}) > 0`, { unit: "s", decimals: 1 }],
          ["Last run took", `max by (workflow) (${newest("scheduler_workflow_last_run_duration_seconds")})`, { unit: "s", decimals: 1 }],
          ["Total run time", `sum by (workflow) (scheduler_workflow_run_duration_seconds_sum{${WF}})`, { unit: "s", decimals: 0 }],
        ], { sortBy: "Mean run time" })],
    ]),
  ];
  emit("dagron-run-time.json", {
    uid: "dagron-run-time", title: "dagron — run time",
    description: "How long runs take: the pipeline view for CI and batch work. Run wall time overall and per workflow, whether it is drifting, and how much of it is waiting for a worker.",
    panels, extraVars: [WORKFLOW_VAR], time: "now-3h",
  });
}

// ── 4. Workflow jobs ────────────────────────────────────────────────────────
{
  const K = "scheduler_task_duration_seconds";
  const active = (status, by) => `sum by (${by}) (${ds("scheduler_workflow_active_tasks", "workflow, status", status ? `status=~"${status}"` : "", WF)})`;
  const failureRatio = (r) => `sum(rate(scheduler_tasks_failed_total{${SEL}}[${r}])) / (sum(rate(scheduler_tasks_succeeded_total{${SEL}}[${r}])) + sum(rate(scheduler_tasks_failed_total{${SEL}}[${r}])))`;
  const cacheRatio = (r) => `sum(rate(scheduler_cache_hits_total{${SEL}}[${r}])) / (sum(rate(scheduler_cache_hits_total{${SEL}}[${r}])) + sum(rate(scheduler_tasks_dispatched_total{${SEL}}[${r}])))`;
  const panels = [
    ...line(4, [
      [4, stat("Jobs running", "Tasks a worker is executing now.", [target(`sum(${ds("scheduler_tasks", "status", 'status="running"')})`, "running")], { decimals: 0, noValue: "0", steps: [["blue", null]] })],
      [4, stat("Jobs ready", "Tasks whose dependencies are met and that are waiting for a worker.", [target(`sum(${ds("scheduler_queue_depth", "")})`, "ready")], { decimals: 0, noValue: "0", steps: [["green", null], ["yellow", 50], ["red", 500]] })],
      [4, stat("Waiting for approval", "Approval gates parked until someone approves or rejects them.", [target(`sum(${ds("scheduler_tasks", "status", 'status="awaiting_approval"')})`, "gates")], { decimals: 0, graphMode: "none", noValue: "0", steps: [["green", null], ["yellow", 1]] })],
      [4, stat("Job failure ratio", "Tasks that exhausted their retries, as a share of all finished tasks, over the last 5 minutes.", [target(failureRatio("5m"), "failed")], { unit: "percentunit", decimals: 1, noValue: "no finished jobs", steps: [["green", null], ["yellow", 0.01], ["red", 0.05]] })],
      [4, stat("Job duration p95", "95th percentile task wall time over the last 5 minutes.", [target(quantile(0.95, K, "5m"), "p95")], { unit: "s", noValue: "no finished jobs" })],
      [4, stat("Retries", "Task attempts rescheduled for another try in the selected time range.", [target(incRange("scheduler_tasks_retried_total"), "retried")], { decimals: 0, graphMode: "none", steps: [["green", null], ["yellow", 1]] })],
    ]),
    row("Jobs in progress"),
    ...line(9, [
      [12, timeseries("Jobs in progress by workflow", "Tasks that have not finished, by the workflow they belong to.",
        [target(active("", "workflow"), "{{workflow}}")], { decimals: 0, stack: true, fill: 30, min: 0 })],
      [12, timeseries("Jobs in progress by status", "Pending is waiting on dependencies, ready is waiting for a worker, running is executing, awaiting_approval is parked at a gate.",
        [target(active("", "status"), "{{status}}")], { decimals: 0, stack: true, fill: 30, min: 0, overrides: STATUS_COLORS })],
    ]),
    ...line(9, [
      [14, table("Jobs in progress", "One row per workflow with unfinished tasks.",
        [["workflow", "Workflow"]],
        [
          ["Running", active("running", "workflow"), { decimals: 0 }],
          ["Ready", active("ready", "workflow"), { decimals: 0, steps: [["green", null], ["yellow", 20]] }],
          ["Pending", active("pending", "workflow"), { decimals: 0 }],
          ["Awaiting approval", active("awaiting_approval", "workflow"), { decimals: 0, steps: [["green", null], ["yellow", 1]] }],
        ], { sortBy: "Running" })],
      [10, timeseries("Ready jobs by runner class", "The dispatch backlog split by the pool that must serve it. A class no engine serves only ever grows.",
        [target(`sum by (runner_class) (${ds("scheduler_ready_tasks_by_class", "runner_class")})`, "{{runner_class}}")], { decimals: 0, stack: true, fill: 30, min: 0 })],
    ]),
    row("Job outcomes"),
    ...line(8, [
      [12, timeseries("Jobs per second", "Dispatched, finished and retried attempts.",
        [target(rate("scheduler_tasks_dispatched_total"), "dispatched"), target(rate("scheduler_tasks_succeeded_total"), "succeeded"), target(rate("scheduler_tasks_failed_total"), "failed"), target(rate("scheduler_tasks_retried_total"), "retried")],
        { unit: "ops", overrides: [fixed("dispatched", "blue"), fixed("succeeded", "green"), fixed("failed", "red"), fixed("retried", "orange")] })],
      [12, timeseries("Job failure ratio", "Failed over failed plus succeeded. The dashed lines are 1% and 5%.",
        [target(failureRatio(RI), "failure ratio")], { unit: "percentunit", min: 0, softMax: 0.1, steps: [["green", null], ["yellow", 0.01], ["red", 0.05]] })],
    ]),
    ...line(8, [
      [12, timeseries("Job duration", "Task wall time from claim to finish, measured in the worker.", quantiles(K), { unit: "s", fill: 0 })],
      [12, heatmap("Job duration distribution", "Finished tasks per duration bucket. The top bucket is 300 seconds; longer tasks land in +Inf.", K)],
    ]),
    row("Why jobs fail"),
    ...line(9, [
      [12, timeseries("Failed attempts by disposition", "Who owns the failure. Infrastructure is hardware, storage or a lost node; application is the job's own code or configuration; platform is preemption, walltime or cancellation; unknown is a recognised symptom with no clear owner.",
        [target(`sum by (disposition) (rate(scheduler_task_faults_by_disposition_total{${SEL}}[${RI}]))`, "{{disposition}}")], { unit: "ops", stack: true, fill: 30 })],
      [12, barGauge("Failed attempts by class", "Classified failed attempts in the selected time range, largest first. An attempt is counted whether or not it was retried; a failure whose output matches no signature is not counted here.",
        [instant(`sort_desc(sum by (class) (increase(scheduler_task_faults_total{${SEL}}[$__range])) > 0)`, "{{class}}")])],
    ]),
    ...line(8, [
      [12, timeseries("Cache hits", "Tasks answered from the memoization cache instead of running, and that as a share of all tasks resolved.",
        [target(rate("scheduler_cache_hits_total"), "cache hits /s"), target(cacheRatio(RI), "hit ratio")],
        { unit: "ops", overrides: [{ matcher: { id: "byName", options: "hit ratio" }, properties: [
          { id: "unit", value: "percentunit" }, { id: "custom.axisPlacement", value: "right" }, { id: "min", value: 0 }, { id: "max", value: 1 }, { id: "custom.fillOpacity", value: 0 },
        ] }] })],
      [12, timeseries("Oldest ready job by runner class", "How long the longest-waiting task in each class has been ready. The dashed line is five minutes.",
        [target(`max by (runner_class) (${ds("scheduler_ready_oldest_age_seconds", "runner_class")})`, "{{runner_class}}")], { unit: "s", fill: 0, min: 0, steps: [["green", null], ["red", 300]] })],
    ]),
  ];
  emit("dagron-jobs.json", {
    uid: "dagron-jobs", title: "dagron — workflow jobs",
    description: "The task level: which jobs are running, ready or parked and for which workflow, how fast they finish, how often they fail, and what kind of failure it is.",
    panels, extraVars: [WORKFLOW_VAR],
  });
}

// ── 5. Dead letter queue ────────────────────────────────────────────────────
{
  const bySource = ds("scheduler_dead_letters_by_source", "source");
  const age = ds("scheduler_dead_letters_oldest_age_seconds", "source");
  const panels = [
    ...line(4, [
      [5, stat("Dead letters parked", "Submissions in the dead-letter store, waiting to be redriven or deleted.", [target(`sum(${ds("scheduler_dead_letters", "")})`, "parked")], { decimals: 0, colorMode: "background", steps: [["green", null], ["red", 1]] })],
      [5, stat("Oldest dead letter", "How long the oldest parked submission has been waiting.", [target(`max(${age})`, "oldest")], { unit: "s", graphMode: "none", noValue: "nothing parked", steps: [["green", null], ["yellow", 3600], ["red", 86400]] })],
      [5, stat("New in range", "Submissions the selected engines dead-lettered in the selected time range.", [target(incRange("scheduler_dead_letters_total"), "new")], { decimals: 0, graphMode: "none", steps: [["green", null], ["red", 1]] })],
      [5, stat("Sources affected", "Distinct sources with at least one parked dead letter.", [target(`count(${bySource} > 0)`, "sources")], { decimals: 0, graphMode: "none", noValue: "0", steps: [["green", null], ["yellow", 1]] })],
      [4, stat("Runs refused in range", "Runs turned away at admission (disk floor or closed gate) in the selected time range. These are refused, not dead-lettered.",
        [target(`${incRange("scheduler_admission_refused_disk_total")} + ${incRange("scheduler_admission_refused_gate_total")}`, "refused")], { decimals: 0, graphMode: "none", steps: [["green", null], ["yellow", 1]] })],
    ]),
    row("What is parked"),
    ...line(9, [
      [12, timeseries("Dead letters parked by source", "The dead-letter store by the source that produced each row. It falls when rows are redriven or deleted.",
        [target(`sum by (source) (${bySource})`, "{{source}}")], { decimals: 0, stack: true, fill: 30, min: 0 })],
      [12, table("Dead letters by source", "Only the 20 largest sources are named; the rest are summed as \"other\".",
        [["source", "Source"]],
        [
          ["Parked", `sum by (source) (${bySource})`, { decimals: 0, steps: [["green", null], ["red", 1]] }],
          ["Oldest", `max by (source) (${age})`, { unit: "s", decimals: 0, steps: [["green", null], ["yellow", 3600], ["red", 86400]] }],
        ], { sortBy: "Parked" })],
    ]),
    ...line(8, [
      [12, timeseries("Oldest dead letter by source", "Age of the oldest parked row from each source. A line that only climbs is a queue nobody is working.",
        [target(`max by (source) (${age})`, "{{source}}")], { unit: "s", fill: 0, min: 0, steps: [["green", null], ["yellow", 3600], ["red", 86400]] })],
      [12, pie("Share by source", "Where the parked dead letters came from.", [instant(`sum by (source) (${bySource})`, "{{source}}")])],
    ]),
    row("Arrivals"),
    ...line(8, [
      [12, timeseries("New dead letters", "Submissions dead-lettered in each interval, by the engine that did it.",
        [target(incStep("scheduler_dead_letters_total", "instance"), "{{instance}}")], { decimals: 0, min: 0, bars: true, stack: true })],
      [12, timeseries("Runs refused at admission", "Runs turned away before they were created: the free-disk floor (DAGRON_MIN_FREE_BYTES) or a closed admission gate (DAGRON_ADMISSION_FILE).",
        [target(incStep("scheduler_admission_refused_disk_total"), "disk floor"), target(incStep("scheduler_admission_refused_gate_total"), "gate closed")], { decimals: 0, min: 0, bars: true })],
    ]),
  ];
  emit("dagron-dead-letters.json", {
    uid: "dagron-dead-letters", title: "dagron — dead letter queue",
    description: "Submissions that could not become runs: how many are parked, where they came from, how long they have waited, and how fast new ones arrive.",
    panels, time: "now-3h",
  });
}

// ── 6. Instance metrics ─────────────────────────────────────────────────────
{
  const poolRatio = `sum by (instance) (scheduler_db_pool_in_use{${SEL}}) / sum by (instance) (scheduler_db_pool_max{${SEL}})`;
  const cpu = (r) => `sum by (instance) (rate(process_cpu_seconds_total{${SEL}}[${r}]))`;
  const panels = [
    ...line(4, [
      [4, stat("Engines up", "Selected engines that answered their last scrape.", [target(`count(scheduler_uptime_seconds{${SEL}})`, "engines")], { decimals: 0, graphMode: "none", noValue: "0", steps: [["red", null], ["green", 1]] })],
      [4, stat("CPU", "CPU cores the selected engines are using, over the last 5 minutes.", [target(`sum(rate(process_cpu_seconds_total{${SEL}}[5m]))`, "cores")], { unit: "suffix: cores", decimals: 3, noValue: "Linux only" })],
      [4, stat("Memory", "Resident memory of the selected engines.", [target(`sum(process_resident_memory_bytes{${SEL}})`, "rss")], { unit: "bytes", noValue: "Linux only" })],
      [4, stat("Open files", "Busiest engine's open file descriptors as a share of its limit.", [target(`max(process_open_fds{${SEL}} / process_max_fds{${SEL}})`, "fds")], { unit: "percentunit", decimals: 1, noValue: "Linux only", steps: [["green", null], ["yellow", 0.7], ["red", 0.9]] })],
      [4, stat("DB pool in use", "Busiest engine's share of its datastore connection pool.", [target(`max(${poolRatio})`, "pool")], { unit: "percentunit", decimals: 0, steps: [["green", null], ["yellow", 0.7], ["red", 0.9]] })],
      [4, stat("Shortest uptime", "The most recently started of the selected engines.", [target(`min(scheduler_uptime_seconds{${SEL}})`, "uptime")], { unit: "s", graphMode: "none" })],
    ]),
    row("Process"),
    ...line(8, [
      [12, timeseries("CPU by engine", "CPU cores each engine process is using (user plus system).", [target(cpu(RI), "{{instance}}")], { unit: "suffix: cores", decimals: 3, min: 0 })],
      [12, timeseries("Memory by engine", "Resident memory: what each engine process holds in RAM.",
        [target(`process_resident_memory_bytes{${SEL}}`, "{{instance}}")], { unit: "bytes", min: 0 })],
    ]),
    ...line(8, [
      [8, timeseries("Open file descriptors", "Descriptors each engine has open. The stat above compares the busiest engine with its limit; running out stops the engine accepting connections and starting tasks.",
        [target(`process_open_fds{${SEL}}`, "{{instance}}")], { decimals: 0, min: 0, fill: 0 })],
      [8, timeseries("Threads", "Operating-system threads in each engine process.", [target(`process_threads{${SEL}}`, "{{instance}}")], { decimals: 0, min: 0, fill: 0 })],
      [8, timeseries("CPU per dispatched task", "CPU seconds the engine spends for each task it dispatches. Rising with flat load is the scheduler doing more work per task.",
        [target(`sum(rate(process_cpu_seconds_total{${SEL}}[${RI}])) / sum(rate(scheduler_tasks_dispatched_total{${SEL}}[${RI}]))`, "cpu seconds per task")], { unit: "s", min: 0, fill: 0 })],
    ]),
    row("Datastore connections"),
    ...line(8, [
      [12, timeseries("DB pool saturation", "Connections checked out over the pool ceiling, per engine. The dashed line is the 90% alert threshold.",
        [target(poolRatio, "{{instance}}")], { unit: "percentunit", min: 0, max: 1, steps: [["green", null], ["red", 0.9]] })],
      [12, timeseries("DB connections", "Open, in use and the configured ceiling, summed over the selected engines.",
        [target(`sum(scheduler_db_pool_connections{${SEL}})`, "open"), target(`sum(scheduler_db_pool_in_use{${SEL}})`, "in use"), target(`sum(scheduler_db_pool_max{${SEL}})`, "max")], { decimals: 0, min: 0, fill: 0 })],
    ]),
    row("Availability and scrape"),
    ...line(8, [
      [8, stateTimeline("Engine up", "Whether Prometheus could scrape each engine.",
        [target(`up{${SEL}}`, "{{instance}}")], [{ type: "value", options: { 0: { text: "down", color: "red", index: 0 }, 1: { text: "up", color: "green", index: 1 } } }], [["red", null], ["green", 1]])],
      [8, timeseries("Engine uptime", "Seconds since each engine booted. A drop to zero is a restart.", [target(`scheduler_uptime_seconds{${SEL}}`, "{{instance}}")], { unit: "s", fill: 0, min: 0, legendCalcs: ["lastNotNull"] })],
      [8, timeseries("Scrape duration", "How long /metrics takes to answer. The datastore gauges are queried on every scrape, so this follows datastore latency.",
        [target(`scrape_duration_seconds{${SEL}}`, "{{instance}}")], { unit: "s", fill: 0, min: 0 })],
    ]),
  ];
  emit("dagron-instance.json", {
    uid: "dagron-instance", title: "dagron — instance metrics",
    description: "Each engine process as a process: CPU, memory, file descriptors, threads, database connections, uptime and scrape health.",
    panels,
  });
}

// ── 7. Health and latency ───────────────────────────────────────────────────
{
  const D = "scheduler_dispatch_latency_seconds";
  const W = "scheduler_result_wait_seconds";
  const T = "scheduler_reconcile_tick_seconds";
  const C = "scheduler_claim_batch_size";
  const under = (le) => `sum(rate(${D}_bucket{${SEL}, le="${le}"}[${RI}])) / sum(rate(${D}_count{${SEL}}[${RI}]))`;
  const failureRatio5m = `sum(rate(scheduler_tasks_failed_total{${SEL}}[5m])) / (sum(rate(scheduler_tasks_succeeded_total{${SEL}}[5m])) + sum(rate(scheduler_tasks_failed_total{${SEL}}[5m])))`;
  const panels = [
    ...line(4, [
      [3, stat("Engines up", "Selected engines that answered their last scrape.", [target(`sum(up{${SEL}})`, "up")], { decimals: 0, graphMode: "none", colorMode: "background", noValue: "0", steps: [["red", null], ["green", 1]] })],
      [3, stat("Clock", "Wall-clock confidence stamped on new runs. Worst value across the selected engines.", [target(`max(scheduler_clock_confidence{${SEL}} == 1) or max(scheduler_clock_confidence{${SEL}})`, "clock")], { colorMode: "background", graphMode: "none", mappings: clockMap, steps: [["green", null], ["red", 1], ["yellow", 2]] })],
      [3, stat("Claims", "Whether DAGRON_PRESSURE_FILE is holding new task claims at zero on any selected engine.", [target(`max(scheduler_claims_paused{${SEL}})`, "claims")], { colorMode: "background", graphMode: "none", mappings: onOff("claiming", "paused"), steps: [["green", null], ["red", 1]] })],
      [3, stat("Job failures", "Tasks that exhausted their retries, as a share of all finished tasks, over the last 5 minutes.", [target(failureRatio5m, "failed")], { unit: "percentunit", decimals: 1, noValue: "no finished jobs", steps: [["green", null], ["yellow", 0.01], ["red", 0.05]] })],
      [3, stat("Dispatch p95", "95th percentile time from a task becoming claimable to being handed to a worker, over the last 5 minutes.", [target(quantile(0.95, D, "5m"), "p95")], { unit: "s", steps: lat(0.5, 2.5), noValue: "no dispatches" })],
      [3, stat("Result p95", "95th percentile time a finished task waits for the reconcile loop to collect its result.", [target(quantile(0.95, W, "5m"), "p95")], { unit: "s", steps: lat(0.25, 1), noValue: "no results" })],
      [3, stat("Tick p95", "95th percentile duration of one reconcile-loop pass. Above one second the loop is falling behind.", [target(quantile(0.95, T, "5m"), "p95")], { unit: "s", steps: lat(0.5, 1) })],
      [3, stat("Queue depth", "Ready tasks waiting for a worker.", [target(`sum(${ds("scheduler_queue_depth", "")})`, "ready")], { decimals: 0, steps: [["green", null], ["yellow", 50], ["red", 500]] })],
    ]),
    row("Health"),
    ...line(6, [
      [8, stateTimeline("Engine up", "Whether Prometheus could scrape each engine.", [target(`up{${SEL}}`, "{{instance}}")],
        [{ type: "value", options: { 0: { text: "down", color: "red", index: 0 }, 1: { text: "up", color: "green", index: 1 } } }], [["red", null], ["green", 1]])],
      [8, stateTimeline("Clock confidence per engine", "The confidence each engine stamps on new runs.", [target(`scheduler_clock_confidence{${SEL}}`, "{{instance}}")], clockMap, [["green", null], ["red", 1], ["yellow", 2]])],
      [8, stateTimeline("Claims paused per engine", "Whether the pressure file is holding claims at zero.", [target(`scheduler_claims_paused{${SEL}}`, "{{instance}}")], onOff("claiming", "paused"), [["green", null], ["red", 1]])],
    ]),
    ...line(8, [
      [8, timeseries("Backlog", "Ready tasks waiting for a worker, and the age of the oldest one.",
        [target(`sum(${ds("scheduler_queue_depth", "")})`, "queue depth"), target(`max(scheduler_ready_oldest_age_seconds{${SEL}})`, "oldest ready task")],
        { decimals: 0, min: 0, overrides: [{ matcher: { id: "byName", options: "oldest ready task" }, properties: [{ id: "unit", value: "s" }, { id: "custom.axisPlacement", value: "right" }, { id: "custom.fillOpacity", value: 0 }] }] })],
      [8, timeseries("Run deadlines", "Hard: runs failed by run_timeout_secs. Soft: SLA alerts raised by deadline, where the run keeps going.",
        [target(incStep("scheduler_runs_deadline_exceeded_total"), "failed by run_timeout_secs"), target(incStep("scheduler_deadline_alerts_total"), "deadline alert raised")], { decimals: 0, min: 0, bars: true })],
      [8, timeseries("Leaked workloads and clock steps", "Remote jobs the engine could not tear down, pods or containers the fleet sweep deleted because their task was no longer live, and wall-clock steps the detector caught.",
        [target(incStep("scheduler_external_orphans_total"), "remote jobs abandoned"), target(incStep("scheduler_orphan_workloads_reaped_total"), "workloads reaped"), target(incStep("scheduler_clock_steps_total"), "clock steps")], { decimals: 0, min: 0, bars: true })],
    ]),
    row("Dispatch latency"),
    ...line(8, [
      [12, timeseries("Dispatch latency", "Task became claimable (scheduled_at) to handed to a worker: claim wait, tick pacing and dispatch preparation. A retry's clock starts at its due time, so backoff is not counted.", quantiles(D), { unit: "s", fill: 0 })],
      [12, heatmap("Dispatch latency distribution", "Dispatches per bucket in each interval. A second band above the main one means some tasks are waiting a full tick.", D)],
    ]),
    ...line(8, [
      [12, timeseries("Dispatches inside a latency budget", "Share of dispatches that finished within each bound. Read this as an SLO: pick the line that matches your target and alert when it drops.",
        [target(under("0.01"), "within 10 ms"), target(under("0.1"), "within 100 ms"), target(under("1"), "within 1 s")], { unit: "percentunit", min: 0, max: 1, fill: 0, legendCalcs: ["lastNotNull", "min"] })],
      [12, timeseries("Result wait", "Executor finished to result drained by the reconcile loop. Rises when the loop is busy.", quantiles(W), { unit: "s", fill: 0 })],
    ]),
    row("Reconcile loop"),
    ...line(8, [
      [8, timeseries("Reconcile tick duration", "One pass of recover, advance, dispatch, collect and reap. The dashed line is the one-second alert threshold.", quantiles(T), { unit: "s", fill: 0, steps: [["green", null], ["red", 1]] })],
      [8, heatmap("Reconcile tick distribution", "Ticks per duration bucket in each interval.", T)],
      [8, timeseries("Claim batch size", "Tasks taken per non-empty claim. A p95 pinned at WORKER_COUNT means every claim fills the pool and work is queueing behind it.",
        [target(`sum(rate(${C}_sum{${SEL}}[${RI}])) / sum(rate(${C}_count{${SEL}}[${RI}]))`, "mean"), target(quantile(0.95, C), "p95")], { unit: "short", fill: 0, min: 0 })],
    ]),
  ];
  emit("dagron-health-latency.json", {
    uid: "dagron-health-latency", title: "dagron — health and latency",
    description: "Is dagron itself healthy and fast: engines up, clock and claim state, backlog, and how long the scheduler takes to dispatch a task, collect a result and complete a reconcile pass.",
    panels,
  });
}
console.log("wrote 7 dashboards to", outDir);
