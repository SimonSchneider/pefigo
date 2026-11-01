(function() {
    function getThemeColor(cssVar, fallback) {
        try {
            var v = getComputedStyle(document.documentElement).getPropertyValue(cssVar).trim();
            return v || fallback;
        } catch (_) {
            return fallback;
        }
    }

    function getThemeOpts() {
        var baseContent = getThemeColor('--color-base-content', '#333');
        var baseBg = getThemeColor('--color-base-100', '#fff');
        var tooltipBorder = getThemeColor('--color-base-300', '#ccc');
        return {
            baseContent: baseContent,
            baseBg: baseBg,
            tooltipBorder: tooltipBorder,
            textStyle: { color: baseContent, textBorderWidth: 0 },
            axisLabel: { color: baseContent, textBorderWidth: 0 }
        };
    }

    function formatThousands(val) {
        var n = Math.round(val);
        var neg = n < 0;
        var s = Math.abs(n).toString();
        var pre = s.length % 3 || 3;
        var out = s.slice(0, pre);
        for (var i = pre; i < s.length; i += 3) {
            out += ',' + s.slice(i, i + 3);
        }
        return neg ? '-' + out : out;
    }

    // A row of date line labels is MARKLINE_ROW_HEIGHT high. Labels below the
    // chart start MARKLINE_BOTTOM_OFFSET under it, past the axis labels.
    var MARKLINE_ROW_HEIGHT = 15;
    var MARKLINE_BOTTOM_OFFSET = 24;

    var dom = document.getElementById('dashboard-forecast-chart');
    if (!dom) return;

    var chart = echarts.init(dom);
    var statusEl = document.getElementById('forecast-status');
    var failedEl = document.getElementById('forecast-failed');
    var durationSelect = document.getElementById('forecast-duration');

    // Client state, rebuilt from scratch on every EventSource (re)connect.
    // series maps an Account Type ID to its name, colour and history points,
    // which every setup replaces, and to its forecast points, which every
    // Forecast Run clears and refills. seriesOrder is the order of the latest
    // setup. Every point is [date, balance], with the date as "YYYY-MM-DD": the
    // history ends today and the forecast starts after it, so they join in
    // date order.
    var series = {};
    var seriesOrder = [];
    var allMarklines = [];
    var hasSetup = false;
    var renderPending = false;

    // getMaxDate is the last date to show, as "YYYY-MM-DD" so it compares with
    // the points' dates.
    function getMaxDate() {
        var years = parseInt(durationSelect ? durationSelect.value : '10', 10);
        if (years === 0) return null; // max = no limit
        var d = new Date();
        d.setFullYear(d.getFullYear() + years);
        return d.toISOString().slice(0, 10);
    }

    function showLoading() {
        if (statusEl) statusEl.classList.remove('hidden');
    }

    function hideLoading() {
        if (statusEl) statusEl.classList.add('hidden');
    }

    // The failure badge's tooltip (DaisyUI, via data-tip) carries the message.
    function showFailure(message) {
        if (!failedEl) return;
        failedEl.setAttribute('data-tip', message || 'The forecast failed');
        failedEl.classList.remove('hidden');
    }

    function hideFailure() {
        if (!failedEl) return;
        failedEl.classList.add('hidden');
        failedEl.setAttribute('data-tip', '');
    }

    function resetState() {
        series = {};
        seriesOrder = [];
        allMarklines = [];
        hasSetup = false;
        hideLoading();
        hideFailure();
    }

    function toPoint(p) {
        return [p.date, p.balance];
    }

    // applySetup replaces the series and the marklines with the setup's. A
    // series the setup no longer has is removed and a new one starts without
    // forecast points. The others keep theirs: only a starting Forecast Run
    // clears forecast points, and the server sends its started right after.
    function applySetup(data) {
        var next = {};
        seriesOrder = (data.series || []).map(function(e) {
            var prev = series[e.id];
            next[e.id] = {
                name: e.name,
                color: e.color || '#666',
                history: (e.history || []).map(toPoint),
                forecast: prev ? prev.forecast : []
            };
            return e.id;
        });
        series = next;
        allMarklines = data.marklines || [];
        hasSetup = true;
    }

    // Forecast points of an Account Type the latest setup has no series for
    // are dropped.
    function addForecastPoint(p) {
        var entry = series[p.id];
        if (!entry) return;
        entry.forecast.push(toPoint(p));
    }

    function clearForecasts() {
        seriesOrder.forEach(function(id) {
            series[id].forecast = [];
        });
    }

    function buildFilteredSeries(theme) {
        var maxDate = getMaxDate();
        function inRange(day) { return !maxDate || day <= maxDate; }
        var lines = seriesOrder.map(function(id) {
            var s = series[id];
            return {
                id: 'series:' + id,
                name: s.name,
                type: 'line',
                data: s.history.concat(s.forecast).filter(function(p) { return inRange(p[0]); }),
                showSymbol: false,
                smooth: false,
                lineStyle: { color: s.color },
                itemStyle: { color: s.color }
            };
        });
        var marklines = [];
        // Each row of labels, above or below the chart, alternates between two
        // heights so that names of nearby lines don't overlap.
        var rows = { top: 0, bottom: 0 };
        allMarklines.forEach(function(m, idx) {
            if (!inRange(m.date)) return;
            var color = m.color || theme.baseContent;
            marklines.push({
                id: 'markline:' + idx,
                name: m.name,
                type: 'line',
                markLine: {
                    symbol: ['none', 'none'],
                    data: [{
                        xAxis: m.date,
                        lineStyle: { color: color, type: 'dashed' },
                        label: marklineLabel(m, m.bottom ? rows.bottom++ : rows.top++, color)
                    }]
                }
            });
        });
        return lines.concat(marklines);
    }

    // marklineLabel names a date line above the chart, or below the axis
    // labels for a line labelled at the bottom, such as an Event's. n is the
    // line's place in its row.
    function marklineLabel(m, n, color) {
        if (m.bottom) {
            return {
                position: 'start',
                offset: [0, n % 2 === 0 ? MARKLINE_BOTTOM_OFFSET : MARKLINE_BOTTOM_OFFSET + MARKLINE_ROW_HEIGHT],
                formatter: m.name,
                color: color,
                textBorderWidth: 0
            };
        }
        return {
            offset: [0, n % 2 !== 0 ? 0 : -MARKLINE_ROW_HEIGHT],
            formatter: m.name,
            color: color,
            textBorderWidth: 0
        };
    }

    // hasBottomMarklines reports whether a date line in range is labelled
    // below the chart, which then needs room under its axis labels.
    function hasBottomMarklines() {
        var maxDate = getMaxDate();
        return allMarklines.some(function(m) { return m.bottom && (!maxDate || m.date <= maxDate); });
    }

    function updateChart() {
        var theme = getThemeOpts();
        // replaceMerge drops series that are no longer in the option, so a
        // reconnect or a shorter duration never leaves stale lines behind.
        // The legend's data array is replaced wholesale on every call.
        chart.setOption({
            backgroundColor: theme.baseBg,
            tooltip: {
                trigger: 'axis',
                backgroundColor: theme.baseBg,
                borderColor: theme.tooltipBorder,
                extraCssText: 'box-shadow: 0 2px 4px rgba(0,0,0,0.15);',
                formatter: function(params) {
                    var items = params.filter(function(p) {
                        return p.value && p.value[1] !== 0;
                    });
                    if (items.length === 0) return '';
                    items.sort(function(a, b) { return b.value[1] - a.value[1]; });
                    var header = items[0].value[0];
                    var total = items.reduce(function(sum, p) { return sum + p.value[1]; }, 0);
                    var lines = items.map(function(p) {
                        return '<div style="display:flex;justify-content:space-between;gap:16px">' +
                            '<span>' + p.marker + ' ' + p.seriesName + '</span>' +
                            '<span style="font-weight:500">' + formatThousands(p.value[1]) + '</span></div>';
                    });
                    var totalLine = '<div style="display:flex;justify-content:space-between;gap:16px;border-top:1px solid ' + theme.tooltipBorder + ';margin-top:4px;padding-top:4px">' +
                        '<span>Total</span><span style="font-weight:700">' + formatThousands(total) + '</span></div>';
                    return header + '<br/>' + lines.join('') + totalLine;
                }
            },
            legend: {
                data: seriesOrder.map(function(id) { return { name: series[id].name }; }),
                type: 'scroll',
                bottom: 0,
                textStyle: theme.textStyle
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: hasBottomMarklines() ? 90 : '15%',
                containLabel: true
            },
            xAxis: {
                type: 'time',
                max: getMaxDate() || undefined,
                axisLabel: theme.axisLabel
            },
            yAxis: {
                type: 'value',
                axisLabel: theme.axisLabel
            },
            series: buildFilteredSeries(theme)
        }, { replaceMerge: ['series'] });
    }

    // Live snapshots arrive one at a time, so renders are coalesced to at most
    // one per frame. Nothing renders until the first setup, so a reconnect
    // keeps showing the previous chart until the new one is ready.
    function scheduleRender() {
        if (!hasSetup || renderPending) return;
        renderPending = true;
        requestAnimationFrame(function() {
            renderPending = false;
            if (hasSetup) updateChart();
        });
    }

    var evtSource = new EventSource('/dashboard/forecast/stream');

    evtSource.addEventListener('open', function() {
        resetState();
    });

    // The setup arrives first on every connection, and again before every
    // Forecast Run that starts while connected.
    evtSource.addEventListener('setup', function(event) {
        applySetup(JSON.parse(event.data) || {});
        scheduleRender();
    });

    evtSource.addEventListener('started', function() {
        clearForecasts();
        showLoading();
        hideFailure();
        scheduleRender();
    });

    evtSource.addEventListener('snapshot', function(event) {
        addForecastPoint(JSON.parse(event.data));
        scheduleRender();
    });

    evtSource.addEventListener('done', function() {
        hideLoading();
    });

    evtSource.addEventListener('failed', function(event) {
        var message = '';
        try {
            message = (JSON.parse(event.data) || {}).message || '';
        } catch (_) {}
        hideLoading();
        showFailure(message);
    });

    chart.on('legendselectchanged', function(params) {
        seriesOrder.forEach(function(id) {
            var name = series[id].name;
            chart.dispatchAction({
                type: params.selected[name] ? 'legendSelect' : 'legendUnSelect',
                name: name
            });
        });
    });

    if (durationSelect) {
        durationSelect.addEventListener('change', function() { scheduleRender(); });
    }

    window.addEventListener('resize', function() { chart.resize(); });
    window.addEventListener('themechange', function() { scheduleRender(); chart.resize(); });
})();
