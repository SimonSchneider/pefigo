# The model is the view model

Templates use `model` types directly, and the page loaders (`Get*PageData`) and the views they return (`{Page}View`) live in `model`, in the file of the feature they show. The architecture review of 2026-10 (#125) weighed moving page assembly into its own package so that `model.Service` offers only domain operations. For a single-user app whose page loaders are short chains of Service calls, that package would be a third place to change for every feature without hiding anything. The `view` package used to re-export about 80 model types as aliases. They added nothing, so they are gone.

## Considered Options

- **A page package** owning the page loaders, views, form prefill strings and chart data: the Service would offer only domain operations, but every feature would span model, page and view, and chart data would move out of the model, where #121 put it on purpose.
- **Keep the aliases**: templates read `Account` rather than `model.Account`, but every new type needs an alias line.

## Consequences

- The model may prepare data for display: form prefill strings, whether a page edits or creates, chart data with its JSON tags, totals and groupings. It knows no URLs or markup: links and CSS belong to the view and controller.
- Each page loader lists the accounts or categories it needs. The calls repeat across page loaders but not within one page load, so don't merge them into a shared lookup.
