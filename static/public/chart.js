function getThemeColor(cssVar, fallback) {
    try {
        const v = getComputedStyle(document.documentElement).getPropertyValue(cssVar).trim();
        return v || fallback;
    } catch (_) {
        return fallback;
    }
}

const colors = [
    `#D32F2F`,
    `#F57C00`,
    `#FFB300`,
    `#FBC02D`,
    `#388E3C`,
    `#00897B`,
    `#00ACC1`,
    `#1976D2`,
    `#303F9F`,
    `#7B1FA2`,
    `#C2185B`,
    `#795548`,
    `#616161`,
];

const myChart = echarts.init(document.getElementById('main'), null, {
    // renderer: 'svg'
});
window.addEventListener('resize', function () {
    myChart.resize();
});
const batchInterval = 100;

const series = {};

function getColor(idx) {
    return colors[idx % colors.length];
}

function lightenColor(color) {
    const [r, g, b] = color.match(/\w\w/g).map(c => parseInt(c, 16));
    return `rgba(${r}, ${g}, ${b}, 0.2)`;
}

function getChartThemeOpts() {
    const textColor = getThemeColor('--color-base-content', '#333333');
    const bgColor = getThemeColor('--color-base-100', 'transparent');
    const borderColor = getThemeColor('--color-base-300', '#ccc');
    const axisLineColor = getThemeColor('--color-base-300', '#eee');
    const textStyle = {
        color: textColor,
        textBorderWidth: 0,
        textBorderColor: 'transparent',
    };
    return {
        backgroundColor: bgColor,
        textColor: textColor,
        textStyle: textStyle,
        tooltip: {
            backgroundColor: bgColor,
            borderColor: borderColor,
            textStyle: { color: textColor, textBorderWidth: 0 },
        },
        legend: { textStyle: textStyle },
        xAxis: {
            axisLine: { lineStyle: { color: axisLineColor } },
            axisLabel: { color: textColor, textBorderWidth: 0 },
            nameTextStyle: textStyle,
            splitLine: { lineStyle: { color: axisLineColor } },
        },
        yAxis: {
            axisLine: { lineStyle: { color: axisLineColor } },
            axisLabel: { color: textColor, textBorderWidth: 0 },
            nameTextStyle: textStyle,
            splitLine: { lineStyle: { color: axisLineColor } },
        },
    };
}

function applyChartTheme() {
    var t = getChartThemeOpts();
    myChart.setOption({
        backgroundColor: t.backgroundColor,
        textStyle: t.textStyle,
        tooltip: t.tooltip,
        legend: { textStyle: t.legend.textStyle },
        xAxis: {
            axisLine: t.xAxis.axisLine,
            axisLabel: t.xAxis.axisLabel,
            nameTextStyle: t.xAxis.nameTextStyle,
            splitLine: t.xAxis.splitLine,
        },
        yAxis: {
            axisLine: t.yAxis.axisLine,
            axisLabel: { formatter: '{value} kr', color: t.yAxis.axisLabel.color, textBorderWidth: 0 },
            nameTextStyle: t.yAxis.nameTextStyle,
            splitLine: t.yAxis.splitLine,
        },
        dataZoom: [{
            type: 'inside',
            xAxisIndex: 0
        }],
    });
    myChart.resize();
}

window.addEventListener('themechange', applyChartTheme);

var themeOpts = getChartThemeOpts();
myChart.setOption({
    legend: {
        data: [],
        selectedMode: 'multiple',
        type: 'scroll',
        orient: 'vertical',
        bottom: '80px',
        right: 10,
        textStyle: themeOpts.legend.textStyle,
    },
    grid: {
        containLabel: true,
        left: '0',
        right: '0',
        bottom: '50px',
        top: '40px',
    },
    animationDurationUpdate: batchInterval,
    tooltip: Object.assign({
        order: 'valueDesc',
        trigger: 'axis',
        valueFormatter: (value) => `${value.toLocaleString('en-us', { maximumFractionDigits: 0 })} kr`,
    }, themeOpts.tooltip),
    xAxis: {
        type: 'time',
        name: 'Date',
        nameLocation: 'middle',
        nameGap: 30,
        axisLine: themeOpts.xAxis.axisLine,
        axisLabel: themeOpts.xAxis.axisLabel,
        nameTextStyle: themeOpts.xAxis.nameTextStyle,
        splitLine: themeOpts.xAxis.splitLine,
    },
    yAxis: {
        type: 'value',
        name: 'Balance',
        nameLocation: 'middle',
        nameGap: 100,
        axisLabel: { formatter: '{value} kr', color: themeOpts.yAxis.axisLabel.color, textBorderWidth: 0 },
        axisLine: themeOpts.yAxis.axisLine,
        nameTextStyle: themeOpts.yAxis.nameTextStyle,
        splitLine: themeOpts.yAxis.splitLine,
    },
    dataZoom: [{
        type: 'inside',
        xAxisIndex: 0
    }],
    series: [],
    backgroundColor: themeOpts.backgroundColor,
    textStyle: themeOpts.textStyle,
});

const addPointToSeries = (seriesName, day, balance) => (series[seriesName].data || []).push([day, balance]);
const addDataPoint = (dp) => {
    addPointToSeries(dp.id, dp.day, dp.balance);
    addPointToSeries(`${dp.id}_min`, dp.day, dp.lowerBound);
    addPointToSeries(`${dp.id}_max`, dp.day, dp.upperBound - dp.lowerBound);
}
const runUpdateBatch = () => myChart.setOption({ series: Object.values(series) });
const intervalUpdate = setInterval(runUpdateBatch, batchInterval);

let idx = 0;

const addSeries = (data) => {
    const color = data.color || getColor(idx);
    series[data.id] = {
        id: data.id,
        name: data.name,
        type: 'line',
        data: [],
        showSymbol: false,
        smooth: false,
        group: data.name,
        lineStyle: {
            color,
        },
        itemStyle: {
            color,
        },
    }
    series[`${data.id}_min`] = {
        id: `${data.id}_min`,
        name: `${data.name} min`,
        type: 'line',
        data: [],
        lineStyle: { opacity: 0 },
        stack: `${data.id}-confidence-band`,
        symbol: 'none',
        showSymbol: false,
        smooth: false,
        group: data.name,
        tooltip: {
            show: false,
        },
        label: {
            show: false,
        },
    }
    series[`${data.id}_max`] = {
        id: `${data.id}_max`,
        name: `${data.name} max`,
        type: 'line',
        data: [],
        lineStyle: { opacity: 0 },
        stack: `${data.id}-confidence-band`,
        showSymbol: false,
        smooth: false,
        group: data.name,
        areaStyle: {
            color: lightenColor(color)
        },
        tooltip: {
            show: false,
        },
        label: {
            show: false,
        }
    }
    idx++;
}

myChart.on('legendselectchanged', function (params) {
    Object.values(series).forEach(v => {
        myChart.dispatchAction({
            type: params.selected[v.group] ? 'legendSelect' : 'legendUnSelect',
            name: v.name,
        });
    })
});

const evtSource = new EventSource(`/chart/stream${window.location.search}`);
evtSource.addEventListener('setup', (event) => {
    const data = JSON.parse(event.data);
    data.entities.forEach(e => {
        addSeries(e);
        e.snapshots.forEach(s => addDataPoint(s));
    })
    const today = new Date();
    const today2 = new Date();
    today2.setDate(today2.getUTCDate() + 1000);
    const themeText = getThemeColor('--color-base-content', '#333333');
    // Each row of labels, above or below the chart, alternates between two
    // heights so that names of nearby lines don't overlap.
    const rows = { top: 0, bottom: 0 };
    const hasBottom = data.marklines.some(m => m.bottom && (!data.max || m.date <= data.max));
    myChart.setOption({
        legend: {
            data: data.entities.map(e => ({ name: e.name })),
        },
        // Labels below the chart go between the axis labels and the axis name.
        xAxis: { max: data.max, nameGap: hasBottom ? MARKLINE_BOTTOM_OFFSET + 2 * MARKLINE_ROW_HEIGHT + 6 : 30 },
        grid: { bottom: hasBottom ? 86 : 50 },
        // A marker's ID is its place, not its name: an Event and a Special Date
        // may share a name, and ECharts rejects series with the same ID.
        series: Object.values(series).concat(data.marklines.map((m, idx) => ({
            id: 'markline:' + idx,
            name: m.name,
            type: 'line',
            markLine: {
                symbol: ['none', 'none'],
                data: [
                    {
                        xAxis: m.date,
                        lineStyle: {
                            color: m.color || themeText,
                            type: 'dashed',
                        },
                        label: marklineLabel(m, m.bottom ? rows.bottom++ : rows.top++, m.color || themeText),
                    }
                ]
            }
        }))),
    })
});

evtSource.addEventListener('balanceSnapshot', (event) => addDataPoint(JSON.parse(event.data)));
evtSource.addEventListener('close', (event) => {
    evtSource.close();
    clearInterval(intervalUpdate);
    runUpdateBatch();
});

// A row of date line labels is MARKLINE_ROW_HEIGHT high. Labels below the chart
// start MARKLINE_BOTTOM_OFFSET under it, past the axis labels.
const MARKLINE_ROW_HEIGHT = 15;
const MARKLINE_BOTTOM_OFFSET = 24;

// marklineLabel names a date line above the chart, or below the axis labels for
// a line labelled at the bottom, such as an Event's. n is the line's place in
// its row.
function marklineLabel(m, n, color) {
    if (m.bottom) {
        return {
            position: 'start',
            offset: [0, n % 2 === 0 ? MARKLINE_BOTTOM_OFFSET : MARKLINE_BOTTOM_OFFSET + MARKLINE_ROW_HEIGHT],
            formatter: m.name,
            color: color,
            textBorderWidth: 0,
        };
    }
    return {
        offset: [0, n % 2 !== 0 ? 0 : -MARKLINE_ROW_HEIGHT],
        formatter: m.name,
        color: color,
        textBorderWidth: 0,
    };
}
