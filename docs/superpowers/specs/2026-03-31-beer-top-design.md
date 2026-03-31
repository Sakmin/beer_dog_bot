# Beer Top Design

## Goal

Add a beer recommendation block to the existing Telegram bot so that:

- every Wednesday survey still sends the two existing polls first;
- immediately after the polls, the bot posts a curated beer message based on the latest BeerHounds availability link;
- users can request the same curated list on demand with `/top_beer`.

## Context

The current project is a single-file `aiogram` bot in [`/Users/sergeysakmin/Desktop/Vs Code/telegram-bot/bot.py`](/Users/sergeysakmin/Desktop/Vs Code/telegram-bot/bot.py). Weekly Wednesday surveys are sent from `send_survey()`, and manual survey triggering is available through `/poll`.

The new behavior must preserve the current survey flow and add an external-data enrichment step that tolerates third-party failures without breaking poll delivery.

## User-Facing Behavior

### Wednesday survey flow

When the scheduled Wednesday survey runs:

1. Send the existing poll `Идем в бар на этой неделе?`
2. Send the existing poll `Когда тебе удобно?`
3. Send a follow-up beer message beginning with:

`Смотри какое интересное пиво я нашел:`

The beer message should include up to five beers for each of these categories:

- `New England IPA`
- `IPA`
- `Sour Ale`
- `Pastry Sour Ale`
- `Безалкогольное`

Each beer line should contain:

- beer name;
- brewery name if available;
- Untappd rating;
- Untappd rating count.

Example formatting target:

`Beer Name - Brewery | Untappd 4.18 | 1,248 ratings`

If some categories have no valid matches, the message should include only categories that have data.

### `/top_beer` command

Add a new `/top_beer` command that sends only the beer recommendation message and does not create polls.

The command should work in private chats and in channels/groups where the bot is present.

## Data Sources

### Source 1: Telegram channel

Use the public HTML version of `https://t.me/s/beerhounds73`.

The bot should scan recent channel posts and find the most recent post that contains a `go.glideapps.com/play/...` link representing the current beer availability page.

### Source 2: Glide availability app

Use the Glide page from the latest Telegram post as the canonical source of beers currently available.

Because Glide serves a JavaScript-heavy app, the parser should be designed as a dedicated extraction layer with graceful failure behavior. The design assumes the bot may need more than one extraction strategy over time.

### Source 3: Untappd

Use Untappd data to enrich the beers found in Glide with:

- canonical beer name;
- brewery;
- style;
- rating;
- total number of ratings.

If official Untappd API credentials are added later, the implementation should support upgrading to API-based enrichment without rewriting the higher-level selection flow.

## Recommended Technical Approach

Use a hybrid approach with fallback:

1. Parse the latest BeerHounds post from public Telegram HTML.
2. Extract the latest Glide URL from that post.
3. Parse the available beer list from Glide.
4. Search Untappd for each available beer and resolve the best match.
5. Categorize beers by style.
6. Rank beers within each category using a weighted score based on rating and rating count.
7. Format the final message.

If any external source fails, the bot should degrade safely:

- for Wednesday survey: still send both polls;
- for `/top_beer`: return a short failure message instead of crashing;
- for partial beer data: return only successfully resolved categories/beers.

## Categorization Rules

The style mapper should classify Untappd styles into the following buckets:

### `New England IPA`

Include styles that clearly indicate:

- `New England IPA`
- `Hazy IPA`
- equivalent NEIPA naming

### `IPA`

Include IPA styles that are not already classified as `New England IPA`.

### `Sour Ale`

Include sour and wild-ale style names except styles that are more specifically classified as pastry sours.

### `Pastry Sour Ale`

Include pastry sour and smoothie-style sour naming patterns.

### `Безалкогольное`

Include non-alcoholic styles.

Matching should be case-insensitive and tolerant to minor style wording differences.

## Ranking Rules

Each category should return at most five beers.

Sorting should not use raw rating alone. It should use a weighted score that balances:

- higher Untappd rating;
- larger number of ratings.

Recommended initial score:

`score = rating * log10(rating_count + 10)`

This favors strong beers with real review volume and avoids overvaluing tiny sample sizes.

If needed, the implementation may refine the score later without changing the message contract.

## Matching Rules

Untappd search/match logic should prefer:

1. high similarity between beer names;
2. high similarity between brewery names;
3. a plausible style match when available.

The resolver should avoid obviously wrong matches where only one generic token overlaps.

## Reliability Requirements

The new feature must not block survey delivery.

Specific requirements:

- scheduled polls must be sent even if Telegram scraping, Glide parsing, or Untappd lookup fails;
- `/top_beer` must catch external failures and respond with a short user-friendly message;
- parsing/enrichment code should use timeouts;
- repeated requests within a short period should reuse cached results when possible to avoid excessive third-party calls.

An in-memory cache is sufficient for the first version.

## Architecture Changes

Keep the existing survey scheduling intact and introduce focused helper units for the beer workflow.

Recommended decomposition:

- Telegram source reader: fetch and parse BeerHounds channel HTML;
- Glide source reader: extract currently available beers from the latest Glide page;
- Untappd client/resolver: search and enrich beer entries;
- style categorizer and ranking logic;
- message formatter;
- orchestration function used by both `send_survey()` and `/top_beer`.

The orchestration flow should return a ready-to-send text message or `None`/error information, so the caller can decide how to handle fallback behavior.

## Error Handling

Expected failure modes:

- Telegram page unavailable;
- no recent Glide link found;
- Glide structure changed;
- Untappd result missing or ambiguous;
- network timeout.

Handling expectations:

- log diagnostic details for operators;
- never crash the bot event loop;
- skip individual beers that cannot be resolved cleanly;
- skip empty categories;
- send no beer block on scheduled survey if the whole enrichment flow fails.

## Testing Strategy

Add tests around deterministic logic and parser behavior using stored HTML fixtures.

Minimum coverage:

- Telegram parser finds the latest Glide URL from sample HTML;
- style categorizer maps styles into the correct buckets;
- ranking function orders beers by weighted score;
- formatter builds the expected Telegram message text;
- orchestration handles partial failures cleanly;
- `/top_beer` command returns a user-friendly fallback on upstream failure.

Because the project currently has no test suite, introducing a small test structure is part of the implementation.

## Open Risks

- Glide may require reverse-engineering of a JS-driven data fetch path, which is the most fragile integration point.
- Public Untappd HTML scraping may be brittle compared with API access.
- Beer name normalization may need iterative tuning once real examples are observed.

## Success Criteria

The feature is complete when:

- Wednesday scheduled surveys still send both existing polls;
- a beer recommendation message is sent after the polls when enrichment succeeds;
- `/top_beer` returns the same recommendation block on demand;
- beers are grouped into the five approved categories;
- each category shows at most five ranked beers;
- failures in third-party parsing do not stop poll delivery.
