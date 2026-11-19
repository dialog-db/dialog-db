;; Tonk's standard library, rust/tonk-core/assets/library/core.yaml
;; (origin/main), redone in this notation. It is not a line-by-line
;; translation: workarounds the notation no longer needs are gone. See
;; notebook.clj for the assumptions. Also assumed here: `action: [...]` right
;; after a concept's head gives the words that say a command, and `role:` on an
;; attribute line is the palette slot a command's field fills.
;;
;; Could not express:
;; - Concept, field and rule descriptions are schema data tonk shows agents; here they are comments.
;; - Templates and handlers name models by pinned URI (model=tonk:blob, tonk:site); kept verbatim.
;; - tonk/load writes site's stored xyz.tonk.site/path: declared twice, once in
;;   `:now`. The layer is part of an attribute's identity, so these are two
;;   attributes, and a rule copies one into the other.
;; - A concept with no fields: diagnose keeps its unused name field as the marker's shape.
;; - element! is named by its tag, though element's first parameter is its hashed entity.
;;
;; Simplified:
;; - invitation joins the authorization and credential concepts, not their four attributes.
;; - new-agent-invite reads agent-handoff for its time instead of restating the attribute.
;; - element's four dictionaries are optional, so it matches an element that declares none.
;; - rename-repository's fields are inline: no palette rule names them (core gives it no verb).
;; - Dropped the history of removed marker fields and moved views (data-invite, fab-pause).
;; - Dropped comments that explain YAML, pins, anchors and lowering.

;;; Views of any model

;; `view`, `event`, `concept`, `command` and `rule` are built in; this library
;; declares only their instances.

;; The default directory view. `tonk:_` stands for any model whose own `show`
;; has no `directory` facet: <tonk-display> with no entity reads the model's
;; facet first and falls back to this. It renders the model's instances in a
;; carousel, each a nested single-entity display that resolves its own view.
;; An instance carries no pointer to its model, so the host threads it through
;; `{dom.host/model}`.
(view! tonk:_
  show: {:directory """
    <wa-carousel class="vertical seamless" pagination orientation="vertical">
      <wa-carousel-item subject={this}>
        <tonk-display entity={this} model={dom.host/model} />
      </wa-carousel-item>
    </wa-carousel>
    """})

;;; Events

;; An event is the seam between a host's input model and a command's domain
;; shape: it names the platform event, whether to suppress its default, and a
;; `where` map from command field to source. A template binds one with
;; `on:<name>=<command>`, which resolves the event `on/<name>`. A source is one
;; of three forms, each distinct so a typo is an error rather than another
;; kind: "{field}" interpolates in the scope of the element the event fired on
;; (`{this}` is the repeat row's subject), ".path" reads a property off the
;; live event as the platform spells it, and "\"text\"" is a literal.

;; Activation. The renderer stamps the subject on every repeat row, so the
;; template needs no `data-*` attribute for it.
(event! on/click
  type: "click"
  where: {:subject "{this}"})

;; Activation with the event's timestamp, so two identical clicks mint
;; distinct entities: a command that creates something needs no nonce field.
(event! on/click-once
  type: "click"
  where: {:subject "{this}"
          :time ".timeStamp"})

;; Form submission. Suppressing the default belongs to the event, which keeps
;; the command a plain value a rule can read.
(event! on/submit
  type: "submit"
  prevent-default: true
  where: {:subject "{this}"})

;; A native control's committed value.
(event! on/change
  type: "change"
  where: {:subject "{this}"
          :value ".currentTarget.value"})

;; A native control's checked state.
(event! on/toggle
  type: "change"
  where: {:subject "{this}"
          :checked ".currentTarget.checked"})

;;; Workspace

;; A lean repo's empty starting canvas, where the `tonk/space` name points
;; until the space chooses a home (see seed/name). Agent invitations open from
;; the host's FABB, so rendering an empty space rotates no credential.
;; `subject` is the replica's subject DID.
(concept! (blank ?b subject: ?subject)
  (dialog.replica/subject ?b is: ?subject as: :entity pick: :last))

(view! blank
  show: {:ui """
    <div class="blank-canvas">
      <style>
        .blank-canvas {
          display: flex;
          flex-direction: column;
          justify-content: center;
          min-height: 100dvh;
          width: 100%;
        }
      </style>
    </div>
    """})

;; Attach a sync remote to the current space. Submitting the sync chip's
;; "Enable sync" form writes it. The worker's CreateSpaceHandler serves this
;; and `space/create` alike, keyed on the shared `name` attribute: two names,
;; one command. The repo exists, so only the attach runs. `name` is the
;; space's local repository name, which <tonk-sync-state> stamps into the
;; form's hidden input on click (the workspace's `{name}` is a display label);
;; `remote` is the sync URL the form submits.
(concept! (space/enable-sync ?c name: ?name remote: ?remote)
  (xyz.tonk.command.create-space/name ?c is: ?name as: :text pick: :last layer: :now)
  (xyz.tonk.command.create-space/remote ?c is: ?remote as: :text pick: :last layer: :now))

;;; Sharing

;; Mint a repository invite. The worker's InviteHandler generates a membership
;; keypair, delegates the origin repository's access to its DID, and writes
;; the public delegation chain as an authorization and the private seed as a
;; credential into the session overlay, never replicated. The repository is
;; not a field: the handler reads the branch the commit landed in.
;;
;; <page-mount> raises `mount` as a CustomEvent. Its timestamp makes each
;; dispatch distinct, so a repeat click re-fires the handler and rotates the
;; credential.
(event! on/invite
  type: "mount"
  where: {:time ".timeStamp"})

(concept! (tonk/invite ?c time: ?time)
  (xyz.tonk.command.invite/time ?c is: ?time as: (:float 64) pick: :last layer: :now))

;; Stamp the requesting tab's site for its current path on this branch.
;; <tonk-site> writes it through the regular transact API instead of a `/site`
;; fetch, at its own `with` context, passing its minted `site:<uuid>` as the
;; command's entity. The worker's LoadHandler matches `path` against this
;; branch's route table and stamps the resolved site, with the captured route
;; params, onto that entity in the branch overlay, superseding in place on
;; navigation.
(concept! (tonk/load ?site path: ?path)
  (xyz.tonk.site/path ?site is: ?path as: :text pick: :last layer: :now))

;; `tonk:pause-sync` lives on the profile branch (profile.yaml): the FAB
;; dispatches it there, carrying the target space.

;; The durable, public half of an invite, keyed by the membership DID: the
;; base58 delegation chain the handler minted, the `?access=` part of the
;; invite URL. It replicates like any fact: a delegation chain is a scoped
;; capability, not a secret. `remote` is a legacy sync-endpoint URL suffix,
;; empty since the endpoint moved into the chain's signed `home.address` meta.
(concept! (authorization ?m proof: ?proof remote: ?remote)
  (xyz.tonk.authorization/proof ?m is: ?proof as: :text pick: :last)
  (xyz.tonk.authorization/remote ?m is: ?remote as: :text pick: :last))

;; The private half, on the same membership DID: the keypair's base58 seed
;; (the `#` fragment of the invite URL) and the complete URL, shortened when
;; the shortcut service answered. Written only into the reactor's session
;; overlay, never to the branch tree, so the secrets stay out of storage. The
;; URL carries the seed, so it is exactly as secret.
(concept! (credential ?m seed: ?seed link: ?link)
  (xyz.tonk.credential/seed ?m is: ?seed as: :text pick: :last)
  (xyz.tonk.credential/link ?m is: ?link as: :text pick: :last))

;; An invite that can be used to join a space: both halves on one membership
;; DID, so the share view renders only once the handler has written both.
;; Views render `link`, the whole URL; `access`, `remote` and `code` are the
;; raw parts, for anything that rebuilds the URL by hand.
(concept! (invitation ?m link: ?link access: ?access remote: ?remote code: ?code)
  (authorization ?m proof: ?access remote: ?remote)
  (credential ?m seed: ?code link: ?link))

;; The invite link, an invitation's default view: a readonly input showing the
;; URL the handler assembled and shortened (a template cannot make that
;; round-trip), and a copy button for the same value.
(view! invitation
  show: {:ui """
    <div class="invite-link">
      <style>
        /* Full-width invite field: the URL input fills the row and the
           copy button rides inside it as a suffix, so it reads as one
           control (matching the wireframe) rather than a narrow box with
           a tiny detached icon. */
        .invite-link { display: block; }
        .invite-link__url { display: block; width: 100%; }
      </style>
      <wa-input
        class="invite-link__url"
        label="Share link"
        readonly
        value="{link}"
      >
        <wa-copy-button
          slot="end"
          class="invite-link__copy"
          value="{link}"
        ></wa-copy-button>
      </wa-input>
    </div>
    """})

;;; Agent handoff

;; Request an agent invitation scoped to the current browser account. It and
;; its response have attributes of their own, apart from ordinary sharing's.
;; The invite controls fire it on mount.
(concept! (tonk/agent-handoff ?h time: ?time)
  (xyz.tonk.agent-handoff/time ?h is: ?time as: (:float 64) pick: :last))

;; The session-only response to a handoff: `status` is the ready marker or a
;; pending or failure explanation, `link` the scoped invitation URL when ready
;; and empty otherwise, `account` the account a legacy handoff required (a
;; profile placeholder for bearer invitations).
(concept! (tonk/agent-handoff-state ?r status: ?status link: ?link account: ?account)
  (xyz.tonk.agent-handoff/status ?r is: ?status as: :text pick: :last)
  (xyz.tonk.agent-handoff/link ?r is: ?link as: :text pick: :last)
  (xyz.tonk.agent-handoff/account ?r is: ?account as: :entity pick: :last))

(view! tonk/agent-handoff-state
  show: {:ui """
    <p data-agent-handoff-status>{status}</p>
    <tonk-display entity={this} model=tonk:connection-invite-controls>
      <span slot="no-entity" hidden></span>
    </tonk-display>
    """})

;; An explicit request for a new invitation. Automatic mount requests carry no
;; `fresh` marker.
(event! on/new-agent-invite
  type: "click"
  where: {:time ".timeStamp"
          :fresh "\"new\""})

;; Create a separate agent invitation when the user asks for one: a handoff
;; that carries the explicit new-invitation marker.
(concept! (tonk/new-agent-invite ?h time: ?time fresh: ?fresh)
  (tonk/agent-handoff ?h time: ?time)
  (xyz.tonk.agent-handoff/fresh ?h is: ?fresh as: :text pick: :last))

;; The session-only action for another scoped invitation. `mode` is a marker
;; only the scoped invitation adapter emits. The controls stay reachable when
;; a transient link is unavailable.
(concept! (tonk/connection-invite-controls ?c mode: ?mode)
  (xyz.tonk.agent-handoff/mode ?c is: ?mode as: :text pick: :last))

(view! tonk/connection-invite-controls
  show: {:ui """
    <tonk-component>
      <script type="tonk/module">
        customElements.get("tonk-agent-invite-controls") ||
          customElements.define("tonk-agent-invite-controls", class extends HTMLElement {
            static observedAttributes = ["mode"];
            connectedCallback() {
              this.observer = new MutationObserver(() => this.render());
              this.observer.observe(this, { childList: true, subtree: true });
              this.afterRegistration = () => {
                if (["account", "activation"].includes(this.getAttribute("mode"))) {
                  this.dispatchEvent(new CustomEvent("mount", { bubbles: true }));
                }
              };
              window.addEventListener("tonk:registration-closed", this.afterRegistration);
              this.onclick = event => {
                const button = event.target.closest("[data-invite-account]");
                if (!button || button.hidden) return;
                globalThis.tonk.register(JSON.stringify({
                  reason: this.getAttribute("mode") === "activation"
                    ? "agent-invite-activation" : "agent-invite-account"
                }));
              };
              this.render();
            }
            disconnectedCallback() {
              this.observer?.disconnect();
              window.removeEventListener("tonk:registration-closed", this.afterRegistration);
              this.onclick = null;
            }
            attributeChangedCallback() { if (this.isConnected) this.render(); }
            render() {
              const mode = this.getAttribute("mode");
              for (const button of this.querySelectorAll("[data-invite-action]")) {
                button.hidden = !button.dataset.inviteAction.split(" ").includes(mode);
              }
              const account = this.querySelector("[data-invite-account]");
              if (account) {
                const label = mode === "activation" ? "verify account" : "create an account or sign in";
                if (account.textContent !== label) account.textContent = label;
              }
            }
          });
      </script>
    </tonk-component>
    <tonk-agent-invite-controls mode={mode} on:invite=tonk:agent-handoff>
      <button class="connection-invite-new" data-invite-account data-invite-action="account activation" hidden>create an account or sign in</button>
      <button class="connection-invite-new" data-invite-action="retry" hidden on:new-agent-invite=tonk:new-agent-invite>try again</button>
      <button class="connection-invite-new" data-invite-action="new" hidden on:new-agent-invite=tonk:new-agent-invite>new invite</button>
      <button class="connection-invite-new" data-invite-action="sync" hidden on:new-agent-invite=tonk:new-agent-invite>turn on sync</button>
    </tonk-agent-invite-controls>
    <style>
      tonk-agent-invite-controls [hidden] { display: none !important; }
      .connection-invite-new {
        min-height: 36px; padding: 0 16px; border: 0; border-radius: 0;
        background: transparent; color: var(--wa-color-text-normal);
        box-shadow: inset 0 0 0 1px currentColor;
        font: 600 13px/1 'IBM Plex Sans Condensed', 'Bahnschrift', 'Arial Narrow', sans-serif;
        letter-spacing: .02em; cursor: pointer;
      }
      .connection-invite-new:hover { background: color-mix(in srgb, currentColor 6%, transparent); }
      .connection-invite-new:focus-visible { outline: 2px solid currentColor; outline-offset: 3px; }
    </style>
    """})

;; An agent invitation joined with the repository's display name, for the
;; copied prompt. `account` is what a legacy handoff required; bearer
;; invitations leave it unused.
(concept! (tonk/agent-invite ?r name: ?name link: ?link account: ?account)
  (repo/name ?r is: ?name)
  (xyz.tonk.agent-invite/link ?r is: ?link as: :text pick: :last)
  (xyz.tonk.agent-invite/account ?r is: ?account as: :entity pick: :last))

;; Why the latest invite request produced no shareable link, written into the
;; session overlay by the worker: a stable refusal class, a human-readable
;; reason, and the timestamp of the request it answers. The empty canvas's and
;; empty sheet's agent-link panels fall back to it behind tonk/agent-invite:
;; while neither exists they show a pending label, which a refusal replaces
;; with the worker's reason.
(concept! (share/blocked ?s blocked: ?blocked detail: ?detail time: ?time)
  (xyz.tonk.share/blocked ?s is: ?blocked as: :text pick: :last)
  (xyz.tonk.share/detail ?s is: ?detail as: :text pick: :last)
  (xyz.tonk.share/time ?s is: ?time as: (:float 64) pick: :last))

(view! share/blocked
  show: {:ui """
    <div class="local-invite-notice">
      <style>
        .local-invite-notice {
          display: flex;
          flex-direction: column;
          gap: 7px;
          width: min(432px, 100%);
          --edge-ink: #38182a;
          --edge-soft: #5b4953;
          --edge-panel: #f7f6f5;
          --edge-ring: rgba(56,24,42,.28);
          color: var(--edge-soft);
          font-family: "IBM Plex Sans", Helvetica, Arial, sans-serif;
        }
        :root.wa-dark .local-invite-notice {
          --edge-ink: #e2dfdd;
          --edge-soft: #c8c3bf;
          --edge-panel: #1b1718;
          --edge-ring: rgba(226,223,221,.28);
        }
        .local-invite-notice__label {
          min-height: 36px;
          padding: 9px 12px;
          display: flex;
          align-items: flex-end;
          background: var(--edge-panel);
          box-shadow: 0 0 0 1px var(--edge-ring);
          font-size: 12px;
          font-weight: 600;
          text-transform: lowercase;
        }
        .local-invite-notice p {
          margin: 0;
          min-height: 36px;
          padding: 12px 14px;
          background: var(--edge-panel);
          box-shadow: 0 0 0 1px var(--edge-ring);
          font-size: 13px;
          line-height: 1.55;
          text-wrap: pretty;
        }
      </style>
      <span class="local-invite-notice__label">sharing unavailable</span>
      <p>{detail}</p>
    </div>
    """})

;; Only a ready response yields an agent prompt. The prompt's attributes
;; differ from the response's, or a pending response with an empty link would
;; satisfy the prompt concept directly.
(rule! (assert (tonk/agent-invite ?r name: ?name link: ?link account: ?account))
  (when (tonk/repository ?r name: ?name)
        (tonk/agent-handoff-state ?r status: "ready" link: ?link account: ?account)))

;; The human-facing invitation; the machine instructions live only in the copy
;; button's value.
(view! tonk/agent-invite
  show: {:ui """
    <tonk-component>
      <script type="tonk/module">
        customElements.get("tonk-agent-prompt") ||
          customElements.define("tonk-agent-prompt", class extends HTMLElement {
            static observedAttributes = ["link"];
            connectedCallback() {
              this.observer = new MutationObserver(() => this.render());
              this.observer.observe(this, { childList: true, subtree: true });
              this.render();
            }
            disconnectedCallback() { this.observer?.disconnect(); }
            attributeChangedCallback() { if (this.isConnected) this.render(); }
            render() {
              let mode = "";
              try {
                const hash = new URL(this.getAttribute("link")).hash;
                mode = /^#tonk-agent-v[12]=/.test(hash) ? "scoped" : "unsupported";
              } catch (_) {}
              for (const block of this.querySelectorAll("[data-agent-mode]")) {
                block.hidden = block.dataset.agentMode !== mode;
              }
              let page;
              try { page = new URL(this.getAttribute("link")); } catch (_) {}
              const local = page && ["localhost", "127.0.0.1", "[::1]"].includes(page.hostname);
              const copy = this.querySelector(".agent-prompt__copy");
              const prompt = copy?.getAttribute("value") || "";
              if (page && page.origin !== "https://tonk.network" && prompt.includes("npx --yes @tonk/cli")) {
                const executable = local ? "tonk" : "npx --yes @tonk/cli";
                const selectedPrompt = prompt
                  .replaceAll("npx --yes @tonk/cli", executable)
                  .replace(
                    `${executable} join '`,
                    `${executable} join --via ${JSON.stringify(page.origin)} '`,
                  );
                copy.setAttribute("value", selectedPrompt);
              }
            }
          });
      </script>
    </tonk-component>
    <tonk-agent-prompt link={link}>
    <div class="agent-prompt">
      <style>
        tonk-agent-prompt [hidden] { display: none !important; }
        tonk-agent-prompt [data-agent-mode] { display: flex; flex-direction: column; gap: var(--wa-space-m); }
        .agent-prompt { padding: var(--wa-space-l); border-radius: var(--wa-border-radius-l); background: var(--wa-color-surface-lowered); display: flex; flex-direction: column; gap: var(--wa-space-m); }
        .agent-prompt p { margin: 0; line-height: var(--wa-line-height-normal); text-wrap: pretty; }
        .agent-prompt__action { display: flex; align-items: stretch; }
        .agent-prompt__copy {
          display: inline-flex;
          align-self: flex-start;
          max-inline-size: 100%;
          --agent-copy-ink: var(--wa-color-neutral-on-loud);
          --agent-copy-surface: var(--wa-color-text-normal);
        }
        .agent-prompt__copy::part(button) {
          display: inline-flex;
          box-sizing: border-box;
          min-width: 144px;
          min-height: 44px;
          padding: 0 16px;
          border: 0;
          border-radius: 0;
          align-items: center;
          justify-content: center;
          background: var(--agent-copy-surface);
          color: var(--agent-copy-ink);
          box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--agent-copy-ink) 28%, transparent);
          font: 600 13px/1 'IBM Plex Sans Condensed', 'Bahnschrift', 'Arial Narrow', sans-serif;
          letter-spacing: .02em;
          text-transform: lowercase;
          transition: scale 150ms ease-out, background-color 150ms ease-out;
        }
        .agent-prompt__copy::part(button):hover {
          background: color-mix(in srgb, var(--agent-copy-ink) 6%, var(--agent-copy-surface));
          color: var(--agent-copy-ink);
        }
        .agent-prompt__copy::part(button):active {
          scale: .96;
          background: color-mix(in srgb, var(--agent-copy-ink) 12%, var(--agent-copy-surface));
          color: var(--agent-copy-ink);
        }
        .agent-prompt__copy::part(button):focus-visible {
          background: var(--agent-copy-surface);
          color: var(--agent-copy-ink);
          outline: 2px solid var(--wa-color-text-normal);
          outline-offset: 3px;
        }
        .agent-prompt__copy::part(copy-icon),
        .agent-prompt__copy::part(success-icon),
        .agent-prompt__copy::part(error-icon) {
          color: inherit;
          animation: none;
        }
        .agent-prompt__copy[disabled]::part(button) { opacity: .4; cursor: not-allowed; scale: 1; }
        .agent-prompt__copy::part(tooltip__base) { display: none; }
        @media (prefers-reduced-motion: reduce) {
          .agent-prompt__copy::part(button) { transition: none; }
          .agent-prompt__copy::part(button):active { scale: 1; }
        }
        .agent-prompt__new {
          min-height: 44px; padding: 0 16px; border: 0; border-radius: 0;
          background: transparent; color: var(--wa-color-text-normal);
          box-shadow: inset 0 0 0 1px currentColor;
          font: 600 13px/1 'IBM Plex Sans Condensed', 'Bahnschrift', 'Arial Narrow', sans-serif;
          letter-spacing: .02em; cursor: pointer;
        }
        .agent-prompt__new:hover { background: color-mix(in srgb, currentColor 6%, transparent); }
        .agent-prompt__new:focus-visible { outline: 2px solid currentColor; outline-offset: 3px; }
        .agent-prompt__note { color: var(--wa-color-text-quiet); font-size: var(--wa-font-size-s); }
      </style>
      <div data-agent-mode="unsupported" hidden>
        <p class="agent-prompt__note">this tool link is no longer supported. reload tonk, then create a new link and use <code>tonk join</code>.</p>
        <button class="agent-prompt__new" on:new-agent-invite=tonk:new-agent-invite>new link</button>
      </div>
      <div data-agent-mode="scoped" hidden>
      <p>copy the prompt and give it to your tool. then tell it what you want to build.</p>
      <p class="agent-prompt__note">anyone with this link can read and change this space until it expires or you revoke it. keep it private. you can close tonk after copying.</p>
      <div class="agent-prompt__action">
        <wa-copy-button class="agent-prompt__copy" copy-label="copy prompt" success-label="prompt copied" error-label="could not copy" value="You're helping build the &quot;{name}&quot; Tonk space.

    Connect to the exact space this prompt came from:

      npx --yes @tonk/cli join '{link}'

    This reusable bearer link contains a separate tool identity and the signed grants for this space. It does not sign the CLI into my account or create a human member. Keep it private. Run the command without CLI account login, browser approval, or account-switch flags. The command prints the chosen local name; use --name if you need a different local alias. Only report connected after it prints &quot;Agent connection confirmed&quot;, which follows a successful pull and acknowledged receipt push. If interrupted, resume without the link using `npx --yes @tonk/cli --space NAME join`. If access expires or is revoked, ask me for a fresh link. Local data and offline edits remain available. Multiple holders of this link share the same invitation authority and receipt; create a new link for an independently revocable identity. The receipt does not establish exclusive use or online presence.

    Then run `npx --yes @tonk/cli --space NAME status` and `npx --yes @tonk/cli help tutorial`. The command binds your original working directory to the connected space; elsewhere, pass --space with the printed local name. Ask me what I want to build. Use `npx --yes @tonk/cli` for each CLI command that changes data, schemas, or views; changes sync directly into this space. Finish with `npx --yes @tonk/cli space home &lt;concept&gt;` to put the result on the space home.">
          <span slot="copy-icon" aria-hidden="true">copy prompt</span>
          <span slot="success-icon" aria-hidden="true">prompt copied</span>
          <span slot="error-icon" aria-hidden="true">could not copy</span>
        </wa-copy-button>
      </div>
      </div>
    </div>
    </tonk-agent-prompt>
    """})

;; The receipt an agent CLI writes once it has pulled this space and sent a
;; reply: a durable acknowledgement, deliberately not an online signal.
(concept! (agent-connection ?a status: ?status)
  (xyz.tonk.agent-connection/status ?a is: ?status as: :text pick: :last))

;; Receipts are access-management data. Connection feedback belongs to the
;; initiating session's FAB, never to the shared space's content.
(view! agent-connection
  show: {:directory """
    <div class="agent-connection-receipts" aria-label="Agent connection receipts">
      <tonk-display entity={this} model=tonk:agent-connection />
    </div>
    """
         :ui """
    <p data-this={this}>agent setup confirmed — this confirms a completed sync, not whether the agent is online.</p>
    """})

;;; Members

;; A member of a space, an account on the "shared with" roster. It reads the
;; Membership and MemberRole facts the worker records on the content branch,
;; which syncs, so the roster converges across the space. `member` is the
;; account's DID, `role` is tonk:founder for the creator or tonk:member for an
;; invite claimer, and `name` is the member's self-asserted display name for
;; this space. Requiring the role makes a row resolve only once it is stamped.
(concept! (member ?row member: ?account role: ?role name: ?name)
  (xyz.tonk.membership/member ?row is: ?account as: :entity pick: :last)
  (xyz.tonk.membership/role ?row is: ?role as: :entity pick: :last)
  (xyz.tonk.membership/name ?row is: ?name as: :text pick: :last))

;; The DID of the member to remove. Its own attribute concept, so the palette
;; rule below can conclude it.
(concept! (expel-member/member ?of is: ?is)
  (xyz.tonk.command.expel-member/member ?of
      is: ?is
      as: :entity
      pick: :last
      role: :object
      layer: :now))

;; Remove a member from this space. The worker's handler does the chain work,
;; and the service refuses a removal minted under a member's `/use` chain, so
;; holding the space is what makes it take effect, not the role on the
;; roster. Promotion is not a form: the FAB dispatches it after the page mints
;; the admin hop under the passkey.
(concept! (member/expel ?c member: ?account)
  action: ["expel" "remove member"]
  (expel-member/member ?c is: ?account))

;; One roster row, a member's default view: the display name with the DID's
;; tail beneath as a stable disambiguator (the full DID is in `title`, and
;; `direction: rtl` clips the head with an ellipsis), and a role badge whose
;; label comes from CSS keyed on `data-role`, so the raw role URI never shows.
(view! member
  show: {:ui """
    <div class="roster__row" data-role={role}>
      <span class="roster__identity">
        <span class="roster__name">{name}</span>
        <span class="roster__did" title={member}><bdi>{member}</bdi></span>
      </span>
      <span class="roster__role" data-role={role}>{role}</span>
    </div>
    """})

;; The roster: every member as a row. The invite panel mounts it with
;; `<tonk-display model=tonk:member>` and no entity, so this directory view
;; iterates them all.
(view! member
  show: {:directory """
    <div class="roster">
      <style>
        /* Share-dialog roster section: a labelled block under the invite
           link. (Styled here because the roster view always mounts with
           it, and the share dialog has no <style> of its own.) */
        .roster-section {
          margin-top: var(--wa-space-l);
        }
        .roster-section__label {
          margin: 0 0 var(--wa-space-2xs);
          font-size: var(--wa-font-size-xs);
          text-transform: uppercase;
          letter-spacing: 0.06em;
          color: var(--wa-color-text-quiet);
        }
        .roster {
          display: flex;
          flex-direction: column;
        }
        /* Each member instance mounts as a nested <tonk-display>; let the
           row fill the width so avatar / did / badge lay out across it. */
        .roster > tonk-display,
        .roster > tonk-display > tonk-view {
          display: block;
        }
        .roster__row {
          display: flex;
          align-items: center;
          gap: var(--wa-space-s);
          padding: var(--wa-space-xs) 0;
          border-bottom: 1px solid var(--wa-color-surface-border);
          font-size: var(--wa-font-size-s);
        }
        .roster > tonk-display:last-child .roster__row {
          border-bottom: none;
        }
        /* The sigil-glyph avatar: a deterministic identity glyph hashed
           from the member DID, CLIPPED to a circle. The sigil renders a
           square SVG; `border-radius:50%` + `overflow:hidden` on the host
           clips it round (matching the wireframe avatars). The sprite is
           supplied by the sealed guest, so it paints inside the opaque
           iframe. */
        /* The DID's LAST 8 chars: `direction:rtl` makes the overflow clip
           the head (with a leading ellipsis), so the tail stays visible.
           `<bdi>` keeps the DID itself rendering left-to-right inside the
           rtl box. Monospace, quiet — a fallback identity until names land. */
        /* The name + DID column. Grows to fill the row, pushing the role
           badge to the right edge. */
        .roster__identity {
          display: flex;
          flex-direction: column;
          flex: 1 1 auto;
          min-width: 0;
        }
        /* The display name — the primary identifier. */
        .roster__name {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          font-size: var(--wa-font-size-s);
          color: var(--wa-color-text-normal);
        }
        .roster__did {
          /* Cap to about the last 8 chars (+ the leading ellipsis); the
             full DID is in `title`. A quiet sub-line under the name. */
          max-width: 9ch;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          direction: rtl;
          text-align: left;
          font-family: var(--wa-font-family-code);
          font-size: var(--wa-font-size-xs);
          color: var(--wa-color-text-quiet);
        }
        /* The raw role URI is hidden; the badge label comes from ::after
           keyed on data-role. Square brutalist chip, matching the wireframe:
           founder is filled (loud), member is outline. */
        .roster__role {
          flex: none;
          font-size: 0;
          line-height: 0;
          color: transparent;
        }
        .roster__role::after {
          display: inline-block;
          font-family: var(--wa-font-family-code);
          font-size: 11px;
          line-height: 1.4;
          letter-spacing: 0.02em;
          text-transform: lowercase;
          padding: 1px 7px;
          border: 1px solid var(--wa-color-text-normal);
          color: var(--wa-color-text-normal);
          content: attr(data-role);
        }
        .roster__role[data-role="tonk:founder"]::after {
          content: "founder";
          background: var(--wa-color-text-normal);
          color: var(--wa-color-surface-default);
        }
        .roster__role[data-role="tonk:admin"]::after {
          content: "admin";
        }
        .roster__role[data-role="tonk:member"]::after {
          content: "member";
          color: var(--wa-color-text-quiet);
          border-color: var(--wa-color-surface-border);
        }
      </style>
      <tonk-display entity={this} model={dom.host/model} />
    </div>
    """})

;;; Transplant

;; A record of this space adopting its history from another subject,
;; re-rooted under a fresh key (typically because the old subject's keys were
;; lost). `tonk space transplant` writes it in the first commit minted under
;; the new subject, so it sits at the origin boundary and the seam stays
;; queryable. `origin` is the old subject's DID entity, the form membership
;; and directory rows use, so the record joins facts still anchored on it.
;; `revision` is the origin's last published head record, byte-exact and
;; operator-signed, so the adopted history's endpoint stays verifiable after
;; the origin's own stores are gone. `tree` is the tree root it adopted.
(concept! (tonk/transplant ?t origin: ?origin revision: ?revision tree: ?tree)
  (xyz.tonk.transplant/origin ?t is: ?origin as: :entity pick: :last)
  (xyz.tonk.transplant/revision ?t is: ?revision as: :bytes pick: :last)
  (xyz.tonk.transplant/tree ?t is: ?tree as: :text pick: :last))

;;; Repository

(concept! (repo/name ?of is: ?is)
  (xyz.tonk.repo/name ?of
      is: ?is
      as: :text
      pick: :last))

(concept! (repo/description ?of is: ?is)
  (xyz.tonk.repo/description ?of
      is: ?is
      as: :text
      pick: :last))

;; The repository's own name and description, stored in its content branch so
;; they travel with it. Its entity is the replica's subject DID. The banner
;; falls back to "Untitled" while a read races ahead of the name.
(concept! (tonk/repository ?r name: ?name description: (option ?d))
  (repo/name ?r is: ?name)
  (repo/description ?r is: ?d))

;; Working context for agents, carried by the repository on the same subject
;; DID, so it syncs with the space rather than belonging to one checkout:
;; durable concepts, workflows, decisions and recurring pitfalls, never
;; credentials, invite links, transient status or one-off completions.
(concept! (tonk/agents-markdown ?of is: ?is)
  (xyz.tonk.repo/agents ?of
      is: ?is
      as: :text
      pick: :last))

;; AGENTS.md context attached to the repository subject DID.
(concept! (tonk/agents ?r markdown: ?markdown)
  (tonk/agents-markdown ?r is: ?markdown))

;;; Agent spaces

;; What kind of space a repository is, when it is a special one: `case:agent`
;; for an agent space. Keyed by the repository's subject DID, so the mark
;; travels with the space's content. An agent space is an ordinary space an
;; account provisioned and keeps the key of; the mark alone tells it apart.
;; Its DID is what invites, contacts and attribution name for the agent. An
;; entity label rather than a boolean: a typo cannot read as another kind, and
;; a second kind needs no new attribute.
(concept! (tonk/agent-space ?r kind: ?kind)
  (xyz.tonk.repo/kind ?r is: ?kind as: :entity pick: :last))

;; Which LLM provider's apps an agent space is for (`claude`, `chatgpt`), on
;; the same subject DID. An agent is offered again only to an app of the same
;; provider, so Claude and ChatGPT never share one.
(concept! (tonk/agent-provider ?r provider: ?provider)
  (xyz.tonk.agent-space/provider ?r is: ?provider as: :text pick: :last))

;; One session key's authorization to act for an agent space, and what a
;; session list renders. The entity is the delegation, the agent space's
;; powerline to the session key, keyed by its blob hash as dialog keyed it
;; when the chain was retained. Dialog decomposes issuer, audience, subject
;; and command onto that entity, so there is no proof field and no session
;; DID field: the delegation's own `dialog.ucan/audience` is the session, and
;; revoking the powerline takes this row with it. The powerline never expires:
;; a session reaches other spaces only through invites, which do, and renewing
;; it would need the account. The session's private key is never recorded.
;;
;; The attributes are the session's own rather than `xyz.tonk.device/*`:
;; sharing them would make every session row a device link and every device
;; link a session.
;;
;; `created-at` is when the session was minted, in unix seconds; the
;; delegation has no validity window to read it from. `title` names the
;; client it was minted for, such as "Claude", so a list renders offline.
;; `reason` is `case:agent-session`, an entity label, so a list filters
;; without opening envelopes.
(concept! (agent/session ?d created-at: ?created title: ?title reason: ?reason)
  (xyz.tonk.agent-session/created-at ?d is: ?created as: (:natural 64) pick: :last)
  (xyz.tonk.agent-session/title ?d is: ?title as: :text pick: :last)
  (xyz.tonk.agent-session/reason ?d is: ?reason as: :entity pick: :last))

;;; Renaming the repository

;; The banner input's commit fires the rename event, which fills this command.
;; The command is an entity of its own, hashed from its fields; the rule reads
;; it to rename the repository it names.
(concept! (tonk/rename-repository ?cmd repository: ?r name: ?name)
  (xyz.tonk.rename-repository/subject ?cmd is: ?r as: :entity pick: :last layer: :now)
  (xyz.tonk.rename-repository/name ?cmd is: ?name as: :text pick: :last layer: :now))

(rule! (assert! (repo/name ?r is: ?name))
  (when (tonk/rename-repository ?cmd repository: ?r name: ?name)))

(event! on/repository-rename
  type: "change"
  where: {:repository ".currentTarget.dataset.subject"
          :name ".currentTarget.value"})

;; The repository-name banner: a borderless input that looks like title text
;; until focused.
(view! tonk/repository
  show: {:ui """
    <inline-editable
      class="workspace__title-input"
      value={name}
      data-subject={this}
      on:repository-rename=tonk/rename-repository
      aria-label="Space name"
    ></inline-editable>
    """})

;;; Self-identity chip

;; The signed-in member's own identity for this space, read from the
;; `state:self` overlay the worker stamps on the space branch, so it reaches
;; the sealed iframe with no profile-branch access. `did` feeds the sigil;
;; `name` is the editable display name.
(concept! (tonk/identity ?self did: ?did name: ?name)
  (xyz.tonk.identity/did ?self is: ?did as: :entity pick: :last)
  (xyz.tonk.identity/name ?self is: ?name as: :text pick: :last))

;; Rename the signed-in member, read from the editable chip's value on commit.
;; No rule: a handler in tonk-worker reacts, since it must reach the profile
;; meta branch. The attribute is the command's own, so a space rename never
;; renames the profile.
(concept! (profile/rename ?c name: ?name)
  (xyz.tonk.command.profile-rename/name ?c is: ?name as: :text pick: :last layer: :now))

;; The repository's name as plain text: a Hub card mounts it with `view=label`
;; inside a per-space `with` context, so each card shows its own space's
;; current name.
(view! tonk/repository
  show: {:label """
    {name}
    """})

;; The browser tab's title: the space chrome mounts it with `view=title`.
;; <tab-title> renders nothing; it pushes the text to the host page, which
;; owns `document.title`. A facet of its own, so the banner, the label and the
;; title never collide under the default `ui`.
(view! tonk/repository
  show: {:title """
    <tab-title text="{name} — Tonk"></tab-title>
    """})

;;; Inspector and portal

;; An inspector tile body: <tonk-inspector> over the space named by `source`.
(concept! (inspector ?i source: ?source)
  (xyz.tonk.inspector/source ?i is: ?source as: :text pick: :last))

;; The imperative escape hatch: <tonk-portal> writes an author's HTML document
;; into a sandboxed iframe, for what templates cannot express (canvas, WebGL,
;; widgets). The content is ordinary data, fetched by a nested
;; `<tonk-display model=portal>`.
(concept! (portal ?p content: ?content)
  (xyz.tonk.portal/content ?p is: ?content as: :text pick: :last))

;; A nested <tonk-display> with no `view` attribute resolves the view of its
;; model.
(view! inspector
  show: {:ui """
    <tonk-inspector source={source} />
    """})

(view! inspector
  show: {:directory """
    <tonk-inspector />
    """})

;; The bridge from a portal to <tonk-portal>. `html:` forces the binding
;; through setAttribute, so attributeChangedCallback fires.
(view! portal
  show: {:ui """
    <tonk-portal html:content={content} />
    """})

;;; Elements and components

;; An author-defined custom element carried as branch data. `(element! tag …)`
;; publishes `id:<tag>` pointing at the entity the body hashes to, and the
;; browser resolves a tag through that name when it first meets it in the DOM.
;; Nothing is registered ahead of time, and repointing the name swaps the
;; element for everything that uses it.
;;
;; The entity is hashed from the whole body, so an element is a value: two
;; bodies with the same description and methods are one element, and changing
;; a method makes another. Hashing a single method mints a one-method element;
;; to edit in place, write the one key on the entity the name points at. The
;; tag is not a field: it lives in the name, which can be repointed.
;;
;; `description` says what the element is for, to people and to agents with
;; only the branch to go on. It reaches the hash too.
;;
;; `method` is keyed by name, each a JS arrow function taking the element
;; first. `connected`, `disconnected` and `adopted` take `(self)`;
;; `attribute-changed` takes `(self, name, before, after)` and is driven by a
;; MutationObserver over every attribute, so there is no observedAttributes.
;; When a definition is replaced under live instances, `released` runs on the
;; outgoing one (undo what `connected` set up) and `swapped` on the incoming
;; one (without it, `connected` runs again). The reserved `define`,
;; `() => class extends HTMLElement { … }`, registers its class instead of the
;; generated wrapper and ignores every other key. Any other key is a method,
;; `el.<name>()`, called through the live table, so re-authoring one changes
;; what its callers compute. A key that would shadow a member of
;; `HTMLElement.prototype` is refused.
;;
;; `getter` and `setter` are property reads, `(self) => …`, and writes,
;; `(self, next) => …`, paired by name: both make the property read-write,
;; either alone read-only or write-only. A re-authored getter changes what a
;; page already reading the property sees.
;;
;; `attribute` holds default attribute values, written onto an instance
;; missing them before `connected` runs. It narrows nothing: every attribute
;; is watched. An empty default is `""`.
;;
;; Each dictionary is zero-or-more, so each is optional: an element with a
;; description alone is complete, and its tag resolves and stays inert.
(concept! (element ?e
           description: ?description
           method: (option ?method)
           getter: (option ?getter)
           setter: (option ?setter)
           attribute: (option ?attribute))
  (xyz.tonk.element/description ?e is: ?description as: :text pick: :last)
  (xyz.tonk.element.method ?e is: ?method as: (:dictionary :text) pick: :all)
  (xyz.tonk.element.getter ?e is: ?getter as: (:dictionary :text) pick: :all)
  (xyz.tonk.element.setter ?e is: ?setter as: (:dictionary :text) pick: :all)
  (xyz.tonk.element.attribute ?e is: ?attribute as: (:dictionary :text) pick: :all))

;; The older shape: one JS module that calls `customElements.define` itself.
;; Superseded by element for new work but still supported, and the two
;; coexist: their attributes differ, so neither's query matches the other's
;; rows, each has its own loader, and `tonk element` lists both. A component
;; has no identity of its own: its entity is hashed from the module, so an
;; edit writes a second row, both mount, and the `customElements.get(name) ||`
;; guard every module carries lets the first to run win. A module runs once
;; per realm; replacing one takes effect on the next page load.
(concept! (component ?c module: ?module)
  (xyz.tonk.component/module ?c is: ?module as: :text pick: :last))

;; The bridge to the <tonk-component> loader. The single-entity view loads one
;; module; templates mount the directory view (a hidden
;; `<tonk-display model=component />` in their shell) to load every component
;; on the branch.
(view! component
  show: {:ui """
    <tonk-component module={module} />
    """})

(view! component
  show: {:directory """
    <div class="component-loader" hidden>
      <tonk-component subject={this} module={module}></tonk-component>
    </div>
    """})

;;; Dialog sessions and branches

;; Database session information. `profile` is the did:key of the persistent
;; identity on this device, `operator` the identifier this session queries and
;; commits with, and `branch` the active branches, several when overlays are
;; used.
(concept! (db/session ?s profile: ?profile operator: ?operator branch: ?branch)
  (dialog.session/profile ?s is: ?profile as: :entity pick: :last)
  (dialog.session/operator ?s is: ?operator as: :entity pick: :last)
  (dialog.session/branch ?s is: ?branch as: :entity pick: :all))

;; A branch on some replica (see tonk/replica). Replicas can create same-named
;; branches at different revisions, so a branch is unique per replica rather
;; than per repository. `edition` is the current revision's causal (Lamport)
;; depth.
(concept! (db/branch ?b
            name: ?name
            replica: ?replica
            tree: ?tree
            revision: ?revision
            edition: ?edition)
  (dialog.branch/name ?b is: ?name as: :text pick: :last)
  (dialog.branch/replica ?b is: ?replica as: :entity pick: :last)
  (dialog.branch/tree ?b is: ?tree as: :entity pick: :last)
  (dialog.branch/revision ?b is: ?revision as: :entity pick: :last)
  (dialog.branch/edition ?b is: ?edition as: :natural pick: :last))

;;; Spaces

;; A repository this profile can open, by its local name. These live on the
;; profile repository's main branch, one per repository this device joined or
;; created; the Hub at `/` reads them there (`with="main@profile:tonk"`).
;; `name` is the local name in the space's `/space/{name}` URL, `subject` the
;; repository's stable subject DID, and `kind` tonk:repository for a real
;; space or tonk:profile for the profile's own replica, which the Hub hides in
;; CSS.
(concept! (space ?s name: ?name subject: ?subject kind: ?kind)
  (xyz.tonk.replica/name ?s is: ?name as: :text pick: :last)
  (xyz.tonk.replica/subject ?s is: ?subject as: :entity pick: :last)
  (xyz.tonk.replica/kind ?s is: ?kind as: :entity pick: :last))

;; The space's default content. The space route mounts
;; `<tonk-display entity={replica} model=tonk/space>`, and the `tonk/space`
;; name points at the blank canvas until the space points it at its own root
;; model, which then takes over the space home.
;;
;; A library's install is reverted and redone on every upgrade, so a name or a
;; route it ships is a command: the rule writes it only where the space has not
;; chosen its own, and the next upgrade reverts that write with the rest.

(concept! (seed/name ?n entity: ?e)
  (xyz.tonk.seed-name/entity ?n is: ?e as: :entity pick: :last layer: :now))

(rule! (assert! (name ?n entity: ?e))
  (when (seed/name ?n entity: ?e)
        (unless (name ?n))))

(seed/name! id:tonk/space entity: tonk:blank)

;; This device's replica of a shared repository: unique per device, so it can
;; carry device-specific metadata about the repository. `subject` is the
;; repository's did:key, `profile` the persistent identity on this device.
(concept! (tonk/replica ?r subject: ?subject profile: ?profile)
  (dialog.replica/subject ?r is: ?subject as: :entity pick: :last)
  (dialog.replica/peer ?r is: ?profile as: :entity pick: :last))

;; This device's live sync status, which the chip renders: sync:idle,
;; sync:pending, sync:offline, sync:local or sync:paused. The worker stamps it
;; into an overlay on the space branch, on the singleton `state:here`, so the
;; sealed chip, which reaches only the space branch, reads it with no replica
;; entity. The durable pause preference is apart from it, on the replica
;; entity, and only the worker reads it.
(concept! (tonk/sync ?here status: ?status)
  (xyz.tonk.sync/status ?here is: ?status as: :entity pick: :last))

;;; Site: a tab's location and what it renders

;; The per-tab entity, a `site:<uuid>` the host mints. The service worker
;; stamps the tab's location and matched route on it and the shell reads them
;; from it; tabs coexist as distinct sites in one branch overlay. `anchor` is
;; the URL hash, `space` the repository name, and `branch` the branch name
;; ("main" by default). `branch-entity` is that branch as an entity, so a view
;; can walk from where the tab is to what that branch follows; the name beside
;; it is a location token. `profile-branch` is the branch the profile is on,
;; reached as `{profile-branch}@profile:tonk`. `concept` is the matched route's
;; model, which the shell mounts. `selection` is the text selected in the
;; tab's page, absent when nothing is; like the rest, it goes with the tab.
(concept! (site ?site
            path: ?path
            anchor: ?anchor
            space: ?space
            branch: ?branch
            replica: ?replica
            branch-entity: ?branch-entity
            profile-branch: ?profile-branch
            route: ?route
            concept: ?concept
            selection: (option ?selection))
  (xyz.tonk.site/path ?site is: ?path as: :text pick: :last)
  (xyz.tonk.site/anchor ?site is: ?anchor as: :text pick: :last)
  (xyz.tonk.site/space ?site is: ?space as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last)
  (xyz.tonk.site/replica ?site is: ?replica as: :entity pick: :last)
  (xyz.tonk.site/branch-entity ?site is: ?branch-entity as: :entity pick: :last)
  (xyz.tonk.site/profile-branch ?site is: ?profile-branch as: :text pick: :last)
  (xyz.tonk.site/route ?site is: ?route as: :entity pick: :last)
  (xyz.tonk.site/concept ?site is: ?concept as: :entity pick: :last)
  (xyz.tonk.site/selection ?site is: ?selection as: :text pick: :last))

;; Report what the page in a tab has selected. Every guest's bootstrap.js
;; reports it as it settles, and the handler records it on the tab's site as
;; `selection`, in the session overlay, so rules and the palette read it as a
;; site fact. `site` is `window.tonk.context.site`, an empty `text` clears the
;; selection, and `time` makes reporting the same selection again re-fire.
(concept! (site/select ?c site: ?site text: ?text time: ?time)
  (xyz.tonk.command.site-select/site ?c is: ?site as: :entity pick: :last layer: :now)
  (xyz.tonk.command.site-select/text ?c is: ?text as: :text pick: :last layer: :now)
  (xyz.tonk.command.site-select/time ?c is: ?time as: (:float 64) pick: :last layer: :now))

;; A site renders the route model it matched. The shell mounts a fixed
;; `<tonk-display entity=site:… model=tonk:site>`, and this view nests the
;; matched `{concept}` on the same site, so the route model resolves there,
;; picking the site's fields, and its own view renders.
(view! site
  show: {:ui """
    <tonk-display entity={this} model={concept} />
    """})

;;; Routing

;; A route maps a path pattern to a route model. The service worker matches
;; the path after the space prefix and stamps the tab's site with `concept`
;; set to that model; the guest mounts it on the site, where the model picks
;; the site fields it needs. Patterns use axum/matchit 0.8 brace syntax. Two
;; route models with the same fields stay apart by their names.

;; A route's entity is hashed from what it maps, so writing the same route
;; twice lands on one entity.
(concept! (route ?r path: ?path concept: ?c)
  (xyz.tonk.route/path ?r is: ?path as: :text pick: :last)
  (xyz.tonk.route/concept ?r is: ?c as: :entity pick: :last))

;; The routes this library ships are seed/route commands, not routes. The rule
;; writes a route for each one whose path no route claims yet. An upgrade
;; reverts everything the previous install wrote, these routes included, so a
;; route the library moved applies and a path the space routed keeps the
;; space's route. A route the space writes for a path the library already
;; routed stands beside the library's until that upgrade.
(concept! (seed/route ?r path: ?path concept: ?c)
  (xyz.tonk.seed-route/path ?r is: ?path as: :text pick: :last layer: :now)
  (xyz.tonk.seed-route/concept ?r is: ?c as: :entity pick: :last layer: :now))

(rule! (assert! (route path: ?path concept: ?c))
  (when (seed/route path: ?path concept: ?c)
        (unless (route path: ?path))))

;;; Seeds: the library a space runs

;; A seed that exists, and where its bytes came from: global, durable, and
;; silent about whether anything installed it. The entity is the content hash
;; of the bytes, so devices fetching one library converge on one entity.
;; `source` is the URL the bytes came from (a custom seed is another URL at
;; the same shape). `replaces` is the installed seed this one would
;; supersede, so an available seed says on its own that it is an update.
(concept! (seed/available ?seed source: ?source replaces: ?replaces)
  (xyz.tonk.seed/source ?seed is: ?source as: :text pick: :last)
  (xyz.tonk.seed/replaces ?seed is: ?replaces as: :entity pick: :last))

;; The seed a space is running, on the same entity as seed/available, so a
;; fetched seed never looks half-installed. A commit records only what it
;; changes, so an install reverts everything first and writes its library in
;; a commit of its own, which then holds all of it. `version` is that
;; commit's version, which the next upgrade reverts. `prior` is the seed this
;; one replaced, or `seed:none` on a first install, so the chain reads
;; uniformly.
(concept! (seed/install ?seed prior: ?prior version: ?version)
  (xyz.tonk.seed/prior ?seed is: ?prior as: :entity pick: :last)
  (xyz.tonk.seed/install-version ?seed is: ?version as: :text pick: :last))

;; How releases before complete installs recorded the seed a space runs. An
;; upgrade still reads it: a space carrying it was installed, at least in
;; part, that way, and moves to a complete install by reverting every install
;; it recorded. A worker from one of those releases finds no install on a
;; space recorded by seed/install and leaves it alone. `version` is a version,
;; not a revision: a revision includes the tree its commit produces, so a fact
;; inside the commit cannot name it, while a version is known before the
;; batch is written. The commit's history lists every claim it wrote.
(concept! (seed/installed ?seed prior: ?prior version: ?version)
  (xyz.tonk.seed/prior ?seed is: ?prior as: :entity pick: :last)
  (xyz.tonk.seed/version ?seed is: ?version as: :text pick: :last))

;;; Route models and their views

;; The space's default page, a bare `/space/{name}/`. It picks the tab's
;; replica, repository and branch off the site and renders the replica (the
;; binder, by default). Like every route view it sets `with=`: the space's
;; content can nest routing consumers, and context reaches them only through
;; stamped `with` attributes.
(concept! (space/route ?site replica: ?replica repo: ?repo branch: ?branch)
  (xyz.tonk.site/replica ?site is: ?replica as: :entity pick: :last)
  (xyz.tonk.site/repo ?site is: ?repo as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last))

(view! space/route
  show: {:ui """
    <tonk-display with="{branch}@{repo}" entity={replica} model=tonk/space />
    """})

;; The workspace shell the bare route resolves to. It renders outer-side (the
;; top document mounts the site, whose view nests this model on the same
;; site), owns the page layout, and mounts the content portal, a sealed guest
;; that renders the space's content; the FAB portal is still to come. It
;; reuses the site's fields: `space` names the routing context.
(concept! (workspace/shell ?site space: ?space branch: ?branch replica: ?replica repo: ?repo)
  (xyz.tonk.site/space ?site is: ?space as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last)
  (xyz.tonk.site/replica ?site is: ?replica as: :entity pick: :last)
  (xyz.tonk.site/repo ?site is: ?repo as: :text pick: :last))

;; The shell mounts the space's content through the `tonk/space` name on the
;; active replica, the blank canvas until something repoints it, in the
;; fill-height `.display-view-slot`. The display sets `with="{branch}@{repo}"`
;; like the other route views: consumers that build worker URLs
;; (<tonk-upload>, the blob view's `{dom.host/with}`) need it on their host
;; chain, which subscriptions inherit but URL builders cannot.
(view! workspace/shell
  show: {:ui """
    <div class="workspace-shell" style="display:flex; flex-direction:column; min-height:100dvh;">
      <div class='display-view-slot'>
        <tonk-display entity={replica} model=tonk/space with="{branch}@{repo}" />
      </div>
    </div>
    """})

;; A model's directory page, `/space/{name}/{model}`. The `{model}` param is
;; stamped on the site and picked here; the view mounts the model's directory
;; (the board strip, the inspector, the tree). Its pattern uses the
;; slash-tolerant extent, so a namespaced model (`tonk/person`) is captured
;; whole.
(concept! (directory/route ?site model: ?model repo: ?repo branch: ?branch)
  (xyz.tonk.site/model ?site is: ?model as: :text pick: :last)
  (xyz.tonk.site/repo ?site is: ?repo as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last))

;; The display carries its own `with`, which it forwards onto the view it
;; mounts, so that view's content resolves the route's space with no routing
;; wrapper.
(view! directory/route
  show: {:ui """
    <tonk-display with="{branch}@{repo}" model={model} />
    """})

;; One artifact's page, `/space/{name}/{entity}@{model}`: the model's view of
;; that one entity.
(concept! (artifact/route ?site entity: ?entity model: ?model repo: ?repo branch: ?branch)
  (xyz.tonk.site/entity ?site is: ?entity as: :entity pick: :last)
  (xyz.tonk.site/model ?site is: ?model as: :text pick: :last)
  (xyz.tonk.site/repo ?site is: ?repo as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last))

(view! artifact/route
  show: {:ui """
    <tonk-display with="{branch}@{repo}" entity={entity} model={model} />
    """})

;; An entity with an explicit view, `/space/{name}/{entity}@{model}!{view}`:
;; the artifact page, with the show facet chosen by the path rather than the
;; model's default.
(concept! (adhoc/route ?site
            entity: ?entity
            model: ?model
            view: ?view
            repo: ?repo
            branch: ?branch)
  (xyz.tonk.site/entity ?site is: ?entity as: :entity pick: :last)
  (xyz.tonk.site/model ?site is: ?model as: :text pick: :last)
  (xyz.tonk.site/view ?site is: ?view as: :text pick: :last)
  (xyz.tonk.site/repo ?site is: ?repo as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last))

(view! adhoc/route
  show: {:ui """
    <tonk-display with="{branch}@{repo}" entity={entity} model={model} view={view} />
    """})

;;; Seeded routes

;; The routes every space starts with. `{name}` captures one segment,
;; `{*name}` a span that may hold `/` up to the next literal or the end (so
;; `{*model}` captures `tonk/person` whole), and several params may share a
;; segment between literals (`@`, `!`). The service worker matches the most
;; specific pattern first, so these never shadow one another. The bare `/`
;; resolves the workspace shell; the rest resolve the page models above.
(seed/route! path: "/" concept: workspace/shell)
(seed/route! path: "/{*entity}@{*model}!{*view}" concept: adhoc/route)
(seed/route! path: "/{*entity}@{*model}" concept: artifact/route)
(seed/route! path: "/{*model}" concept: directory/route)

;;; Tree inspector

;; A marker model for <tonk-tree> over the branch's search-tree index. Its
;; directory page shows the dialog search tree under the database: index and
;; segment nodes, their keys (components color-coded, shared prefixes
;; dimmed), byte sizes and entries. <tonk-tree> resolves its repository from
;; the `with` ancestor and drives itself from the worker's `tree/*` formulas.
;; `name` is unused: a concept needs a field.
(concept! (diagnose ?d name: ?name)
  (db.diagnose/name ?d is: ?name as: :text pick: :last))

;; A directory view: the page carries no entity, and the inspector needs
;; none, since it renders the whole branch's tree.
(view! diagnose
  show: {:directory """
    <tonk-tree></tonk-tree>
    """})

;;; Blobs and assets

;; Legacy metadata for a content-addressed blob. The entity is the content
;; reference (`asset:<base58-hash>`, older ones `blob:`); its size is dialog's
;; `dialog.asset/size`. Nothing in core writes these any more: new content is
;; described as an asset, and a rule makes every asset a blob, so views written
;; against blob keep working, and blobs already recorded and templates that
;; write blob need no change.
(concept! (blob ?b content-type: ?type name: ?name)
  (xyz.tonk.blob/content-type ?b is: ?type as: :text pick: :last)
  (xyz.tonk.blob/name ?b is: ?name as: :text pick: :last))

;; What tonk records about an asset beside dialog's `dialog.asset/size`: its
;; media type, such as image/png, and its file name. `tonk.dialog.*` marks
;; attributes that extend dialog's model; should dialog adopt them, migrating
;; is a rename. Every writer in this release (`tonk blob add`,
;; `!include/asset`, the upload route) writes these. The two rules below join
;; the vocabularies, so nothing is rewritten and views written against either
;; concept see every asset.
(concept! (asset ?a media-type: ?type name: ?name)
  (tonk.dialog.asset/media-type ?a is: ?type as: :text pick: :last)
  (tonk.dialog.asset/name ?a is: ?name as: :text pick: :last))

;; A blob recorded with the legacy attributes is an asset.
(rule! (assert (asset ?b media-type: ?type name: ?name))
  (when (blob ?b content-type: ?type name: ?name)))

;; An asset is a blob, so views that render blobs render it.
(rule! (assert (blob ?a content-type: ?type name: ?name))
  (when (asset ?a media-type: ?type name: ?name)))

;; An asset renders the way a blob does: by the rule above it is one.
(view! asset
  show: {:ui """
    <tonk-display entity={this} model=tonk:blob></tonk-display>
    """})

;; The standard blob renderer, seeded as ordinary branch data so a space can
;; replace it with its own view. Images render inline; any other type renders
;; as a file card with a lazy download. The renderer is author JavaScript in
;; an inert `<script type="tonk/module">`, run once per realm by
;; <tonk-component>. Bytes come through the relayed `window.fetch` as an
;; object URL, since inside the sealed guest a native `src=/api/…` load
;; bypasses the relay and the service worker. The blob route is scoped by the
;; host's `with` ("{branch}@{repo}"), forwarded through `{dom.host/with}`.
(view! blob
  show: {:ui """
    <div subject={this}>
      <tonk-component>
        <script type="tonk/module">
          customElements.get("tonk-blob-media") ||
            customElements.define("tonk-blob-media", class extends HTMLElement {
              static observedAttributes = ["entity", "content-type", "name", "with"];
              #objectUrl = null;
              #generation = 0;
              connectedCallback() { this.render(); }
              attributeChangedCallback() { if (this.isConnected) this.render(); }
              disconnectedCallback() { this.#generation++; delete this.dataset.rendered; this.revoke(); }
              revoke() {
                if (this.#objectUrl) {
                  URL.revokeObjectURL(this.#objectUrl);
                  this.#objectUrl = null;
                }
              }
              url() {
                const scope = this.getAttribute("with") || "";
                const entity = this.getAttribute("entity") || "";
                const at = scope.indexOf("@");
                if (at < 1 || !entity || scope.includes("{")) return null;
                return "/api/repository/" + scope.slice(at + 1) +
                  "/branch/" + scope.slice(0, at) + "/blob/" + entity;
              }
              async objectUrl() {
                const url = this.url();
                if (!url) return null;
                const resp = await window.fetch(url);
                if (!resp.ok) return null;
                return URL.createObjectURL(await resp.blob());
              }
              render() {
                const key = [
                  this.getAttribute("with"),
                  this.getAttribute("entity"),
                  this.getAttribute("content-type"),
                  this.getAttribute("name"),
                ].join("|");
                if (this.dataset.rendered === key) return;
                this.dataset.rendered = key;
                this.#generation++;
                const root = this.shadowRoot || this.attachShadow({ mode: "open" });
                const type = this.getAttribute("content-type") || "";
                const name = this.getAttribute("name") || this.getAttribute("entity") || "";
                this.revoke();
                if (type.startsWith("image/")) {
                  root.innerHTML = "<style>img { max-width: 100%; }</style>" +
                    "<img part=\"media\">";
                  root.querySelector("img").alt = name;
                  const generation = this.#generation;
                  this.objectUrl().then((u) => {
                    if (!u) {
                      delete this.dataset.rendered;
                      return;
                    }
                    if (generation !== this.#generation) {
                      URL.revokeObjectURL(u);
                      return;
                    }
                    this.#objectUrl = u;
                    root.querySelector("img").src = u;
                  }).catch(() => { delete this.dataset.rendered; });
                  return;
                }
                root.innerHTML =
                  "<style>" +
                  "[part=card] { display: flex; align-items: center; gap: 0.75rem; " +
                  "padding: 0.75rem 1rem; border: 1px solid #d0d0d0; border-radius: 8px; " +
                  "max-width: 28rem; font-family: system-ui, sans-serif; }" +
                  "[part=name] { flex: 1; overflow-wrap: anywhere; }" +
                  "[part=badge] { font-size: 0.75rem; color: #666; border: 1px solid #ddd; " +
                  "border-radius: 4px; padding: 0.1rem 0.4rem; white-space: nowrap; }" +
                  "[part=action] { cursor: pointer; }" +
                  "</style>" +
                  "<div part=\"card\"><span part=\"name\"></span>" +
                  "<span part=\"badge\"></span>" +
                  "<button part=\"action\" type=\"button\">Download</button></div>";
                root.querySelector("[part=name]").textContent = name;
                root.querySelector("[part=badge]").textContent = type || "file";
                root.querySelector("[part=action]").addEventListener("click", async () => {
                  let u;
                  try {
                    u = await this.objectUrl();
                  } catch {
                    return;
                  }
                  if (!u) return;
                  const a = document.createElement("a");
                  a.href = u;
                  a.download = name;
                  a.click();
                  URL.revokeObjectURL(u);
                });
              }
            });
        </script>
      </tonk-component>
      <tonk-blob-media entity={this} content-type={content-type} name={name} with={dom.host/with}></tonk-blob-media>
    </div>
    """})

;;; Elements

;; Custom elements carried as branch data, each replacing a Rust element that
;; only ever needed the DOM.

;; Names the browser tab through the host page, which owns `document.title`:
;; a sealed guest cannot set it. It renders nothing, and pushes nothing while
;; `hidden`, so a title a view keeps for a state it is not showing does not
;; win.
(element! tab-title
  description: "Names the browser tab from its `text` attribute, through the host page."
  method: {:push """
    (self) => {
      if (self.hasAttribute('hidden')) return;
      const text = self.getAttribute('text');
      if (!text) return;
      window.tonk?.setTitle?.(text);
    }
    """
           :connected """
    (self) => self.push()
    """
           :attribute-changed """
    (self, name) => {
      if (name === 'text' || name === 'hidden') self.push();
    }
    """})

;; Dispatches the current location as a bubbling `mount` CustomEvent whose
;; `detail` is the parsed URL (`href`, `origin`, `pathname`, `search`, `hash`,
;; and `searchParams` as a plain object), so a view binds a command to it and
;; the handler gets the URL as data rather than reaching for a global. Inside
;; a sealed guest the location is the host's, relayed as
;; `window.tonk.context`. It fires once the enclosing display is bound
;; (`data-bound`), since the binding that listens is installed with it, and
;; again on every `tonk:join-retry`.
(element! page-mount
  description: "Dispatches the parsed current location as a bubbling `mount` event."
  method: {:location """
    (self) => {
      const context = window.tonk?.context;
      const href = context?.origin
        ? `${context.origin}${context.path ?? ''}${context.search ?? ''}${context.hash ?? ''}`
        : window.location.href;
      try {
        const url = new URL(href);
        return {
          href: url.href, origin: url.origin, pathname: url.pathname,
          search: url.search, hash: url.hash,
          searchParams: Object.fromEntries(url.searchParams),
        };
      } catch {
        return {};
      }
    }
    """
           :mount """
    (self) => {
      self.dispatchEvent(new CustomEvent('mount', { bubbles: true, detail: self.location() }));
    }
    """
           :ready """
    (self) => self.closest('tonk-display')?.hasAttribute('data-bound') === true
    """
           :connected """
    (self) => {
      if (!self.__retry) {
        self.__retry = () => self.mount();
        self.addEventListener('tonk:join-retry', self.__retry);
      }
      let fired = false;
      const fire = () => {
        if (fired || !self.ready()) return;
        fired = true;
        self.mount();
      };
      fire();
      if (fired) return;
      const display = self.closest('tonk-display');
      if (!display) return;
      self.__bound?.disconnect();
      self.__bound = new MutationObserver(fire);
      self.__bound.observe(display, { attributes: true, attributeFilter: ['data-bound'] });
    }
    """
           :disconnected """
    (self) => {
      self.__bound?.disconnect();
      self.__bound = null;
    }
    """})

;; A line of text driven like a native input: its `value` property is its
;; text, a double-click makes it editable, Enter commits (a blur, which fires
;; `change` when the text changed), and Escape restores the text the edit
;; began with, so no `change` carries a new value. It holds no policy: the
;; commit rides on the consuming view's binding.
(element! inline-editable
  description: "A contenteditable line of text with input-like `value` and `change`."
  getter: {:value """
    (self) => self.textContent ?? ''
    """}
  setter: {:value """
    (self, value) => {
      if (self !== self.ownerDocument?.activeElement) self.textContent = value ?? '';
    }
    """}
  method: {:connected """
    (self) => {
      self.setAttribute('contenteditable', 'false');
      self.setAttribute('role', 'textbox');
      if (self.__wired) return;
      self.__wired = true;
      let before = '';
      self.addEventListener('dblclick', () => {
        self.setAttribute('contenteditable', 'plaintext-only');
        self.focus();
      });
      self.addEventListener('focus', () => {
        before = self.textContent ?? '';
      });
      self.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          event.preventDefault();
          self.blur();
        } else if (event.key === 'Escape') {
          event.preventDefault();
          self.textContent = before;
          self.blur();
        }
      });
      self.addEventListener('blur', () => {
        const current = self.textContent ?? '';
        self.setAttribute('contenteditable', 'false');
        if (current === before) return;
        self.dispatchEvent(new Event('change', { bubbles: true }));
      });
    }
    """})

;; A frame that can be dragged around the viewport, whatever it holds. A press
;; on the handle (the frame itself unless `handle` names a part) that travels
;; past the dead zone becomes a drag: the frame tracks the pointer, stays
;; inside the viewport, and on release docks to the nearest edge at `inset`
;; pixels, keeping its coordinate along that edge, or only clamps when
;; `dock="none"`. A tap is left alone, so the content's click still happens;
;; the click that ends a drag is swallowed. A resize keeps a docked frame on
;; its edge and any frame inside the viewport. It reflects `dragging`,
;; `docked` (the edge) and `left`/`top` in pixels as attributes, and announces
;; `drag-start` and `drag-end` (detail: `left`, `top`, `edge`), so a view can
;; persist or react.
(element! drag-frame
  description: "Drags its contents around the viewport and docks them to an edge."
  attribute: {:inset "16"
              :dead-zone "4"
              :dock "edges"
              :handle ""}
  method: {:options """
    (self) => {
      const number = (name, fallback) => {
        const value = parseFloat(self.getAttribute(name));
        return Number.isFinite(value) ? value : fallback;
      };
      return {
        inset: number('inset', 16),
        deadZone: number('dead-zone', 4),
        dock: (self.getAttribute('dock') || 'edges') !== 'none',
        handle: self.getAttribute('handle') || '',
      };
    }
    """
           :handle-of """
    (self, target) => {
      const selector = self.options().handle;
      if (!selector) return self;
      const part = target?.closest?.(selector);
      return part && self.contains(part) ? part : null;
    }
    """
           :place """
    (self, left, top) => {
      self.style.left = `${left}px`;
      self.style.top = `${top}px`;
    }
    """
           :snap """
    (self, left, top, edge) => {
      const { inset, dock } = self.options();
      const rect = self.getBoundingClientRect();
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      let x = Math.max(inset, Math.min(left, vw - rect.width - inset));
      let y = Math.max(inset, Math.min(top, vh - rect.height - inset));
      if (!dock) return { left: x, top: y, edge: null };
      if (!edge) {
        const distances = [
          ['left', x], ['right', vw - (x + rect.width)],
          ['top', y], ['bottom', vh - (y + rect.height)],
        ];
        let nearest = distances[0];
        for (const candidate of distances) if (candidate[1] < nearest[1]) nearest = candidate;
        edge = nearest[0];
      }
      if (edge === 'left') x = inset;
      else if (edge === 'right') x = vw - rect.width - inset;
      else if (edge === 'top') y = inset;
      else if (edge === 'bottom') y = vh - rect.height - inset;
      return { left: Math.max(inset, x), top: Math.max(inset, y), edge };
    }
    """
           :settle """
    (self, left, top, edge) => {
      const at = self.snap(left, top, edge);
      self.place(at.left, at.top);
      if (at.edge) self.setAttribute('docked', at.edge);
      else self.removeAttribute('docked');
      self.setAttribute('left', String(Math.round(at.left)));
      self.setAttribute('top', String(Math.round(at.top)));
      return at;
    }
    """
           :connected """
    (self) => {
      if (getComputedStyle(self).position === 'static') self.style.position = 'fixed';
      if (!self.style.touchAction) self.style.touchAction = 'none';
      if (self.__wired) return;
      self.__wired = true;
      let press = null;
      const finish = () => {
        if (!press) return;
        const { moved, touch, pointerId, handle } = press;
        press = null;
        window.removeEventListener('pointermove', move);
        window.removeEventListener('pointerup', finish);
        window.removeEventListener('pointercancel', finish);
        try { (touch ? handle : self).releasePointerCapture(pointerId); } catch {}
        if (!moved) return;
        self.removeAttribute('dragging');
        self.__justDragged = true;
        setTimeout(() => { self.__justDragged = false; }, 0);
        const rect = self.getBoundingClientRect();
        const at = self.settle(rect.left, rect.top);
        self.dispatchEvent(new CustomEvent('drag-end', { bubbles: true, detail: at }));
      };
      const move = (event) => {
        if (!press) return;
        if (event.buttons === 0) { finish(); return; }
        const dx = event.clientX - press.x;
        const dy = event.clientY - press.y;
        if (!press.moved) {
          const { deadZone } = self.options();
          const zone = press.touch ? Math.max(deadZone, 9) : deadZone;
          if (Math.hypot(dx, dy) < zone) return;
          press.moved = true;
          if (!press.touch) { try { self.setPointerCapture(press.pointerId); } catch {} }
          self.removeAttribute('docked');
          self.setAttribute('dragging', '');
          self.dispatchEvent(new CustomEvent('drag-start', {
            bubbles: true, detail: { left: press.left, top: press.top, edge: null },
          }));
        }
        const rect = self.getBoundingClientRect();
        const left = Math.max(0, Math.min(press.left + dx, window.innerWidth - rect.width));
        const top = Math.max(0, Math.min(press.top + dy, window.innerHeight - rect.height));
        self.place(left, top);
      };
      self.addEventListener('pointerdown', (event) => {
        if (event.button !== 0 || press) return;
        const handle = self.handleOf(event.target);
        if (!handle) return;
        const rect = self.getBoundingClientRect();
        press = {
          x: event.clientX, y: event.clientY, left: rect.left, top: rect.top,
          pointerId: event.pointerId, touch: event.pointerType === 'touch',
          moved: false, handle,
        };
        if (press.touch) { try { handle.setPointerCapture(event.pointerId); } catch {} }
        window.addEventListener('pointermove', move);
        window.addEventListener('pointerup', finish);
        window.addEventListener('pointercancel', finish);
      });
      self.addEventListener('click', (event) => {
        if (!self.__justDragged) return;
        self.__justDragged = false;
        event.stopPropagation();
        event.preventDefault();
      }, true);
      self.__resize = () => {
        if (press) return;
        const rect = self.getBoundingClientRect();
        self.settle(rect.left, rect.top, self.getAttribute('docked') || undefined);
      };
      window.addEventListener('resize', self.__resize);
    }
    """
           :disconnected """
    (self) => {
      window.removeEventListener('resize', self.__resize);
    }
    """})

;;; Command palette

;; <command-palette> parses typed text against what commands declare, the way
;; Ubiquity's Parser 2 did (`rust/tonk-lingo` is a port of it; see
;; plan/command-palette-ubiquity.md):
;; - `action:` on a command: the words that say it, each an intent/action fact
;;   on the command.
;; - `role:` on a command's field: the grammatical slot it fills (object, goal,
;;   source, location, time, instrument, format, modifier, alias, or `now` for
;;   the moment it runs). The words that introduce a slot ("to" for goal)
;;   belong to the parser's grammar for a language. An empty slot shows the
;;   field's name ("expel [member]").
;; - Rules say what can fill an entity field: a rule concludes the field's
;;   attribute on the palette's intent. Typed words match those values by the
;;   `label` facet of the concept they are rows of. A text field takes what is
;;   typed.

;; Words that say a command, written on the command itself.
(concept! (intent/action ?command name: ?name)
  (tonk.dialog.intent.action/name ?command is: ?name as: :text pick: :all))

;; Interpret what was typed in the palette, where it was opened. The palette
;; makes an expression per opening and sends this as the line changes; its
;; handler, the parser, records the expression and one intent per command the
;; line could mean, in the session overlay. Rules derive values for a
;; command's fields onto its intent, each on the field's own attribute and
;; scoped to the command (`(intent ?i command: …)`), and `intent/suggest`
;; reads the intents and those values back as readings. `expression` is the
;; opening the page made, `input` exactly what is typed, `site` the tab's
;; site, and `time` makes typing the same line again re-fire.
(concept! (intent/interpret ?c
           expression: ?e
           input: ?input
           site: ?site
           time: ?time)
  (tonk.dialog.intent.interpret/expression ?c is: ?e as: :entity pick: :last layer: :now)
  (tonk.dialog.intent.interpret/input ?c is: ?input as: :text pick: :last layer: :now)
  (tonk.dialog.intent.interpret/site ?c is: ?site as: :entity pick: :last layer: :now)
  (tonk.dialog.intent.interpret/time ?c is: ?time as: (:float 64) pick: :last layer: :now))

;; One opening of the command palette, as interpreted so far, in session
;; facts: what is typed, the tab's site where it was opened, when the line was
;; last interpreted, and the text selected in the page, when there was some.
(concept! (intent/expression ?e input: ?input site: ?site time: ?time selection: (option ?selection))
  (tonk.dialog.intent.expression/input ?e is: ?input as: :text pick: :last)
  (tonk.dialog.intent.expression/site ?e is: ?site as: :entity pick: :last)
  (tonk.dialog.intent.expression/time ?e is: ?time as: :float pick: :last)
  (tonk.dialog.intent.expression/selection ?e is: ?selection as: :text pick: :last))

;; What an expression could mean as one command. Rules derive the command's
;; field values onto it.
(concept! (intent ?i expression: ?e command: ?command)
  (tonk.dialog.intent/expression ?i is: ?e as: :entity pick: :last)
  (tonk.dialog.intent/command ?i is: ?command as: :entity pick: :last))

;; A member as the account member/expel wants. A roster row is the
;; membership; the command takes the account's DID, the row's `member`. This
;; concept's rows are the accounts, so a chosen row's entity is the field
;; value as is. Derived on query: nothing is stored, and it follows the
;; roster. `name` is the display name the member uses in this space.
(concept! (member/account ?account name: ?name)
  (xyz.tonk.member.account/name ?account is: ?name as: :text pick: :last))

;; Every roster member is an account, named by its membership.
(rule! (assert (member/account ?account name: ?name))
  (when (member member: ?account name: ?name)))

(view! member/account
  show: {:label """
    {name}
    """})

;; The library's components a space can install, each beside the others and
;; the space's seed: every library file but core.yaml, which every space runs,
;; and profile.yaml, the profile's. An install is recorded like the seed's and
;; brought up to date the same way when the space mounts; installing one
;; already there updates it, or does nothing when it is current. `name` is the
;; file without `.yaml`.
(concept! (library/component ?c name: ?name)
  (xyz.tonk.library.component/name ?c is: ?name as: :text pick: :last))

(view! library/component
  show: {:label """
    {name}
    """})

(library/component! tonk:library/issue name: "issue")
(library/component! tonk:library/meta name: "meta")
(library/component! tonk:library/notebook name: "notebook")
(library/component! tonk:library/prose name: "prose")
(library/component! tonk:library/table name: "table")
(library/component! tonk:library/version-control name: "version-control")

;; Which component to install, `tonk:library/<name>`. Its own attribute
;; concept, so the palette rule below can conclude it.
(concept! (install-component/component ?of is: ?is)
  (xyz.tonk.command.install-component/component ?of
      is: ?is
      as: :entity
      pick: :last
      role: :object
      layer: :now))

;; The moment it was asked, which makes each dispatch distinct.
(concept! (install-component/time ?of is: ?is)
  (xyz.tonk.command.install-component/time ?of
      is: ?is
      as: (:float 64)
      pick: :last
      role: :now
      layer: :now))

;; Install a library component into this space, or update it.
(concept! (library/install ?c component: ?component time: ?time)
  action: ["install"]
  (install-component/component ?c is: ?component)
  (install-component/time ?c is: ?time))

;; What "install" can be said of: every component.
(rule! (assert (install-component/component ?i is: ?component))
  (when (intent ?i command: library/install)
        (library/component ?component)))

;; What "expel" can be said of: this space's members.
(rule! (assert (expel-member/member ?i is: ?account))
  (when (intent ?i command: member/expel)
        (member/account ?account)))

;; tonk/rename-repository has no verb here: the profile's own rename ("rename
;; space", the FABB's) says the same thing, and the palette reads both
;; branches, so a verb here would offer every rename twice.
