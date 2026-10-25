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

    var dom = document.getElementById('dashboard-forecast-chart');
    if (!dom) return;

    var chart = echarts.init(dom);
    var statusEl = document.getElementById('forecast-status');
    var failedEl = document.getElementById('forecast-failed');
    var durationSelect = document.getElementById('forecast-duration');

    // Client state, rebuilt from scratch on every EventSource (re)connect.
    // series maps an Account Type ID to its history points, which arrive once
    // in the initial payload, and its forecast points, which every Forecast
    // Run clears and refills. seriesOrder keeps the order the series arrived in.
    // Every point is [date, balance], with the date as "YYYY-MM-DD": the history
    // ends today and the forecast starts after it, so they join in date order.
    var series = {};
    var seriesOrder = [];
    var allMarklines = [];
    var setupDone = false;
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
        setupDone = false;
        hideLoading();
        hideFailure();
    }

    function toPoint(p) {
        return [p.date, p.balance];
    }

    function addEntity(e) {
        var id = e.id;
        if (!series[id]) seriesOrder.push(id);
        series[id] = {
            name: e.name,
            color: e.color || '#666',
            history: (e.history || []).map(toPoint),
            forecast: []
        };
    }

    // Forecast points of an Account Type that no longer exists have no series.
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
                        label: {
                            offset: [0, idx % 2 !== 0 ? 0 : -15],
                            formatter: m.name,
                            color: color,
                            textBorderWidth: 0
                        }
                    }]
                }
            });
        });
        return lines.concat(marklines);
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
                bottom: '15%',
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
    // one per frame. Nothing renders until the initial payload's setup-done, so
    // a reconnect keeps showing the previous chart until the new one is ready.
    function scheduleRender() {
        if (!setupDone || renderPending) return;
        renderPending = true;
        requestAnimationFrame(function() {
            renderPending = false;
            if (setupDone) updateChart();
        });
    }

    var evtSource = new EventSource('/dashboard/forecast/stream');

    evtSource.addEventListener('open', function() {
        resetState();
    });

    evtSource.addEventListener('entity', function(event) {
        addEntity(JSON.parse(event.data));
    });

    evtSource.addEventListener('marklines', function(event) {
        allMarklines = JSON.parse(event.data) || [];
    });

    evtSource.addEventListener('setup-done', function() {
        setupDone = true;
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
