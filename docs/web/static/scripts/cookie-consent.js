/**
 * Cookie consent banner + consent-gated RB2B loader for docs.lancedb.com.
 *
 * Mintlify loads every .js file in the content directory on every page, so this
 * file runs site-wide. RB2B is opt-in: its script is never requested until the
 * visitor clicks "Accept". The choice is stored in localStorage and can be
 * changed later from the "Cookie settings" footer link (any link to
 * #cookie-settings) or window.lancedbCookieConsent.open().
 *
 * GA4 and Mintlify's own analytics are NOT gated by this banner.
 */
(function () {
  "use strict";

  if (typeof window === "undefined" || window.lancedbCookieConsent) return;

  // RB2B script ID: app.rb2b.com > Script, the value inside reb2b.load("...").
  var RB2B_KEY = "9NMMZHRRWYNW";

  var STORAGE_KEY = "lancedb-cookie-consent"; // "accepted" | "rejected"
  var POLICY_URL =
    "https://242023405.fs1.hubspotusercontent-na2.net/hubfs/242023405/Website%20Documentation/privacy.pdf";
  var BANNER_ID = "lancedb-cookie-banner";
  var STYLE_ID = "lancedb-cookie-banner-style";
  var SCRIPT_ID = "rb2b-script";
  var SETTINGS_HASH = "#cookie-settings";

  function readConsent() {
    try {
      return window.localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  function writeConsent(value) {
    try {
      window.localStorage.setItem(STORAGE_KEY, value);
    } catch (e) {
      /* storage blocked: banner will simply reappear next visit */
    }
  }

  function hasKey() {
    return RB2B_KEY && RB2B_KEY.indexOf("REPLACE_") !== 0;
  }

  // ---- RB2B -----------------------------------------------------------------
  // Mirrors RB2B's official SPA guidance: remove and re-append the script on
  // every route change so each client-side navigation is recorded.
  function loadRb2b() {
    if (!hasKey() || readConsent() !== "accepted") return;
    var existing = document.getElementById(SCRIPT_ID);
    if (existing) existing.remove();
    var s = document.createElement("script");
    s.id = SCRIPT_ID;
    s.async = true;
    s.src =
      "https://ddwl4m2hdecbv.cloudfront.net/b/" + RB2B_KEY + "/" + RB2B_KEY + ".js.gz";
    document.body.appendChild(s);
  }

  var lastPath = null;
  function onRouteChange() {
    var path = window.location.pathname + window.location.search;
    if (path === lastPath) return;
    lastPath = path;
    loadRb2b();
  }

  function watchRoutes() {
    ["pushState", "replaceState"].forEach(function (method) {
      var original = window.history[method];
      window.history[method] = function () {
        var result = original.apply(this, arguments);
        setTimeout(onRouteChange, 0);
        return result;
      };
    });
    window.addEventListener("popstate", onRouteChange);
  }

  // ---- Banner ---------------------------------------------------------------
  var CSS = [
    "#" + BANNER_ID + "{",
    "  --lcb-bg:#ffffff;--lcb-fg:#1f2328;--lcb-muted:#57606a;--lcb-border:rgba(0,0,0,.12);",
    "  --lcb-accent:#FF6B35;--lcb-accent-fg:#1b0f09;--lcb-link:#B8431A;--lcb-btn2:#f3f4f6;--lcb-btn2-fg:#1f2328;",
    "  position:fixed;z-index:2147483000;left:16px;bottom:16px;max-width:400px;",
    "  box-sizing:border-box;padding:16px 18px;border-radius:12px;",
    "  background:var(--lcb-bg);color:var(--lcb-fg);border:1px solid var(--lcb-border);",
    "  box-shadow:0 8px 30px rgba(0,0,0,.12);",
    "  font:14px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,'Segoe UI',sans-serif;",
    "}",
    "html.dark #" + BANNER_ID + "{",
    "  --lcb-bg:#161616;--lcb-fg:#e6e6e6;--lcb-muted:#a0a0a0;--lcb-border:rgba(255,255,255,.12);",
    "  --lcb-btn2:#262626;--lcb-btn2-fg:#e6e6e6;--lcb-link:#FF8A5C;box-shadow:0 8px 30px rgba(0,0,0,.5);",
    "}",
    "#" + BANNER_ID + " p{margin:0 0 12px;color:var(--lcb-muted);}",
    "#" + BANNER_ID + " strong{display:block;margin-bottom:4px;color:var(--lcb-fg);font-weight:600;}",
    "#" + BANNER_ID + " a{color:var(--lcb-link);text-decoration:underline;}",
    "#" + BANNER_ID + " .lcb-actions{display:flex;gap:8px;}",
    "#" + BANNER_ID + " button{flex:1;cursor:pointer;border:0;border-radius:8px;padding:8px 12px;",
    "  font:inherit;font-weight:600;}",
    "#" + BANNER_ID + " .lcb-reject{background:var(--lcb-btn2);color:var(--lcb-btn2-fg);}",
    "#" + BANNER_ID + " .lcb-accept{background:var(--lcb-accent);color:var(--lcb-accent-fg);}",
    "#" + BANNER_ID + " button:focus-visible{outline:2px solid var(--lcb-accent);outline-offset:2px;}",
    "@media (max-width:480px){#" + BANNER_ID + "{left:16px;right:16px;max-width:none;}}",
    "@media print{#" + BANNER_ID + "{display:none;}}",
  ].join("\n");

  function injectStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = CSS;
    document.head.appendChild(style);
  }

  function closeBanner() {
    var el = document.getElementById(BANNER_ID);
    if (el) el.remove();
  }

  function choose(value) {
    var previous = readConsent();
    writeConsent(value);
    closeBanner();
    if (value === "accepted") {
      lastPath = window.location.pathname + window.location.search;
      loadRb2b();
    } else if (previous === "accepted") {
      // RB2B may already be running in this tab; reload so it stops.
      window.location.reload();
    }
  }

  function openBanner() {
    if (document.getElementById(BANNER_ID)) return;
    injectStyles();

    var el = document.createElement("div");
    el.id = BANNER_ID;
    el.setAttribute("role", "dialog");
    el.setAttribute("aria-live", "polite");
    el.setAttribute("aria-labelledby", BANNER_ID + "-title");
    el.innerHTML =
      '<p><strong id="' + BANNER_ID + '-title">Cookies on LanceDB Docs</strong>' +
      "With your permission, we use a third-party service (RB2B) that sets cookies " +
      "to help us understand which companies and people visit our docs. " +
      "Nothing loads unless you accept. " +
      '<a href="' + POLICY_URL + '" target="_blank" rel="noopener">Privacy policy</a></p>' +
      '<div class="lcb-actions">' +
      '<button type="button" class="lcb-reject">Decline</button>' +
      '<button type="button" class="lcb-accept">Accept</button>' +
      "</div>";

    el.querySelector(".lcb-reject").addEventListener("click", function () {
      choose("rejected");
    });
    el.querySelector(".lcb-accept").addEventListener("click", function () {
      choose("accepted");
    });

    document.body.appendChild(el);
  }

  // Any link to #cookie-settings (e.g. the footer link) reopens the banner.
  function watchSettingsLinks() {
    document.addEventListener(
      "click",
      function (event) {
        var link = event.target && event.target.closest && event.target.closest("a[href]");
        if (!link) return;
        var href = link.getAttribute("href") || "";
        if (href.slice(-SETTINGS_HASH.length) !== SETTINGS_HASH) return;
        event.preventDefault();
        event.stopPropagation();
        openBanner();
      },
      true
    );
  }

  // ---- Init -----------------------------------------------------------------
  window.lancedbCookieConsent = {
    open: openBanner,
    get: readConsent,
    reset: function () {
      try {
        window.localStorage.removeItem(STORAGE_KEY);
      } catch (e) {}
      openBanner();
    },
  };

  function init() {
    watchRoutes();
    watchSettingsLinks();
    var consent = readConsent();
    if (consent === "accepted") {
      onRouteChange();
    } else if (consent !== "rejected") {
      openBanner();
    }
    if (window.location.hash === SETTINGS_HASH) openBanner();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
