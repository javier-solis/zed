use std::ops::Range;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clock::Global;
use collections::{HashMap, HashSet};
use futures::FutureExt as _;
use futures::future::{Shared, join_all};
use gpui::{AppContext as _, AsyncApp, Context, Entity, SharedString, Task};
use language::{Buffer, point_to_lsp};
use lsp::LanguageServerId;
use lsp::request::DocumentLinkResolve;
use rpc::{TypedEnvelope, proto};
use settings::Settings as _;
use text::{Anchor, BufferId, ToOffset as _, ToPointUtf16 as _};

use crate::lsp_command::{GetDocumentLinks, LspCommand as _};
use crate::lsp_store::LspStore;
use crate::project_settings::ProjectSettings;

#[derive(Clone, Debug)]
pub struct LspDocumentLink {
    pub server_id: LanguageServerId,
    pub range: Range<Anchor>,
    pub target: Option<SharedString>,
    pub tooltip: Option<SharedString>,
    pub data: Option<serde_json::Value>,
}

pub(super) type DocumentLinksTask =
    Shared<Task<std::result::Result<Option<Vec<LspDocumentLink>>, Arc<anyhow::Error>>>>;

#[derive(Debug, Default)]
pub(super) struct DocumentLinksData {
    /// Links per server. Sorted by `range.start` inside each bucket so the
    /// viewport-resolver can binary-search rather than scanning everything.
    pub(super) links: HashMap<LanguageServerId, Vec<LspDocumentLink>>,
    links_update: Option<(Global, DocumentLinksTask)>,
    /// `(server_id, index)` pairs currently being resolved. Prevents issuing
    /// duplicate resolve requests while scrolling.
    resolving: HashSet<(LanguageServerId, usize)>,
}

impl DocumentLinksData {
    pub(super) fn remove_server_data(&mut self, server_id: LanguageServerId) {
        self.links.remove(&server_id);
        self.resolving.retain(|(id, _)| *id != server_id);
    }
}

impl LspStore {
    pub fn document_links_for_buffer(&self, buffer_id: BufferId) -> Option<Vec<LspDocumentLink>> {
        let data = self.lsp_data.get(&buffer_id)?;
        let document_links = data.document_links.as_ref()?;
        Some(document_links.links.values().flatten().cloned().collect())
    }

    /// `Some(..)` means the underlying state was actually refreshed; `None`
    /// means the fetch was skipped or failed, and the caller should keep its
    /// previous data.
    pub fn fetch_document_links(
        &mut self,
        buffer: &Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<Option<Vec<LspDocumentLink>>> {
        let version_queried_for = buffer.read(cx).version();
        let buffer_id = buffer.read(cx).remote_id();

        let current_language_servers = self.as_local().map(|local| {
            local
                .buffers_opened_in_servers
                .get(&buffer_id)
                .cloned()
                .unwrap_or_default()
        });

        if let Some(lsp_data) = self.current_lsp_data(buffer_id) {
            if let Some(cached) = &lsp_data.document_links {
                if !version_queried_for.changed_since(&lsp_data.buffer_version) {
                    let has_different_servers =
                        current_language_servers.is_some_and(|current_language_servers| {
                            current_language_servers != cached.links.keys().copied().collect()
                        });
                    if !has_different_servers {
                        return Task::ready(Some(
                            cached.links.values().flatten().cloned().collect(),
                        ));
                    }
                }
            }
        }

        let links_lsp_data = self
            .latest_lsp_data(buffer, cx)
            .document_links
            .get_or_insert_default();
        if let Some((updating_for, running_update)) = &links_lsp_data.links_update {
            if !version_queried_for.changed_since(updating_for) {
                let running = running_update.clone();
                return cx.background_spawn(async move { running.await.ok().flatten() });
            }
        }

        let buffer = buffer.clone();
        let query_version = version_queried_for.clone();
        let new_task = cx
            .spawn(async move |lsp_store, cx| {
                cx.background_executor()
                    .timer(Duration::from_millis(30))
                    .await;

                let fetched = lsp_store
                    .update(cx, |lsp_store, cx| {
                        lsp_store.fetch_document_links_for_buffer(&buffer, cx)
                    })
                    .map_err(Arc::new)?
                    .await
                    .context("fetching document links")
                    .map_err(Arc::new);

                let fetched = match fetched {
                    Ok(fetched) => fetched,
                    Err(e) => {
                        lsp_store
                            .update(cx, |lsp_store, _| {
                                if let Some(lsp_data) = lsp_store.lsp_data.get_mut(&buffer_id) {
                                    if let Some(document_links) = &mut lsp_data.document_links {
                                        document_links.links_update = None;
                                    }
                                }
                            })
                            .ok();
                        return Err(e);
                    }
                };

                lsp_store
                    .update(cx, |lsp_store, cx| {
                        let lsp_data = lsp_store.latest_lsp_data(&buffer, cx);
                        let links_data = lsp_data.document_links.get_or_insert_default();
                        links_data.links_update = None;

                        let Some(mut fetched_links) = fetched else {
                            return None;
                        };

                        let snapshot = buffer.read(cx).snapshot();
                        for links in fetched_links.values_mut() {
                            links.sort_by(|a, b| a.range.start.cmp(&b.range.start, &snapshot));
                        }

                        if lsp_data.buffer_version == query_version {
                            for (server_id, new_links) in fetched_links {
                                links_data.links.insert(server_id, new_links);
                                // Indices in `resolving` refer to the prior
                                // per-server vec, which we just replaced.
                                links_data.resolving.retain(|(id, _)| *id != server_id);
                            }
                        } else if !lsp_data.buffer_version.changed_since(&query_version) {
                            lsp_data.buffer_version = query_version;
                            links_data.links = fetched_links;
                            links_data.resolving.clear();
                        } else {
                            return None;
                        }

                        Some(links_data.links.values().flatten().cloned().collect())
                    })
                    .map_err(Arc::new)
            })
            .shared();

        links_lsp_data.links_update = Some((version_queried_for, new_task.clone()));

        cx.background_spawn(async move { new_task.await.ok().flatten() })
    }

    fn fetch_document_links_for_buffer(
        &mut self,
        buffer: &Entity<Buffer>,
        cx: &mut Context<Self>,
    ) -> Task<anyhow::Result<Option<HashMap<LanguageServerId, Vec<LspDocumentLink>>>>> {
        if let Some((client, project_id)) = self.upstream_client() {
            let request = GetDocumentLinks;
            if !self.is_capable_for_proto_request(buffer, &request, cx) {
                return Task::ready(Ok(None));
            }

            let request_timeout = ProjectSettings::get_global(cx)
                .global_lsp_settings
                .get_request_timeout();
            let request_task = client.request_lsp(
                project_id,
                None,
                request_timeout,
                cx.background_executor().clone(),
                request.to_proto(project_id, buffer.read(cx)),
            );
            let buffer = buffer.clone();
            cx.spawn(async move |weak_lsp_store, cx| {
                let Some(lsp_store) = weak_lsp_store.upgrade() else {
                    return Ok(None);
                };
                let Some(responses) = request_task.await? else {
                    return Ok(None);
                };

                let document_links = join_all(responses.payload.into_iter().map(|response| {
                    let lsp_store = lsp_store.clone();
                    let buffer = buffer.clone();
                    let cx = cx.clone();
                    async move {
                        let server_id = LanguageServerId::from_proto(response.server_id);
                        let links = GetDocumentLinks
                            .response_from_proto(response.response, lsp_store, buffer, cx)
                            .await;
                        (server_id, links)
                    }
                }))
                .await;

                let mut has_errors = false;
                let result = document_links
                    .into_iter()
                    .filter_map(|(server_id, links)| match links {
                        Ok(links) => Some((server_id, links)),
                        Err(e) => {
                            has_errors = true;
                            log::error!(
                                "Failed to fetch document links for server {server_id}: {e:#}"
                            );
                            None
                        }
                    })
                    .collect::<HashMap<_, _>>();
                anyhow::ensure!(
                    !has_errors || !result.is_empty(),
                    "Failed to fetch document links"
                );
                Ok(Some(result))
            })
        } else {
            let links_task =
                self.request_multiple_lsp_locally(buffer, None::<usize>, GetDocumentLinks, cx);
            cx.background_spawn(async move { Ok(Some(links_task.await.into_iter().collect())) })
        }
    }

    /// Deduplicates in-flight resolves via `resolving` so the viewport
    /// resolver and an on-demand hover resolve don't double-issue requests
    /// for the same link.
    pub fn resolve_document_links(
        &mut self,
        buffer: &Entity<Buffer>,
        ranges: &[Range<Anchor>],
        cx: &mut Context<Self>,
    ) -> Task<Vec<LspDocumentLink>> {
        if ranges.is_empty() {
            return Task::ready(Vec::new());
        }
        let buffer_id = buffer.read(cx).remote_id();
        let snapshot = buffer.read(cx).snapshot();
        let offset_ranges = ranges
            .iter()
            .map(|r| r.start.to_offset(&snapshot)..r.end.to_offset(&snapshot))
            .collect::<Vec<_>>();

        let Some(document_links) = self
            .lsp_data
            .get(&buffer_id)
            .and_then(|data| data.document_links.as_ref())
        else {
            return Task::ready(Vec::new());
        };

        let upstream = self.upstream_client();
        let local_servers = if upstream.is_none() {
            document_links
                .links
                .keys()
                .filter_map(|server_id| {
                    let server = self.language_server_for_id(*server_id)?;
                    can_resolve_link(&server.capabilities()).then_some((*server_id, server))
                })
                .collect::<HashMap<_, _>>()
        } else {
            HashMap::default()
        };
        let upstream_capable = upstream.is_some()
            && self.check_if_capable_for_proto_request(buffer, can_resolve_link, cx);
        if upstream.is_none() && local_servers.is_empty() {
            return Task::ready(Vec::new());
        }
        if upstream.is_some() && !upstream_capable {
            return Task::ready(Vec::new());
        }

        let mut seen = HashSet::default();
        let mut to_resolve = Vec::new();
        for (server_id, links) in &document_links.links {
            if upstream.is_none() && !local_servers.contains_key(server_id) {
                continue;
            }
            for (index, link) in links.iter().enumerate() {
                // A link is fully resolved once we have a target and no
                // pending server-side `data` payload to re-submit.
                if link.target.is_some() && link.data.is_none() {
                    continue;
                }
                if document_links.resolving.contains(&(*server_id, index)) {
                    continue;
                }
                let link_start = link.range.start.to_offset(&snapshot);
                let link_end = link.range.end.to_offset(&snapshot);
                let overlaps = offset_ranges
                    .iter()
                    .any(|r| link_start <= r.end && link_end >= r.start);
                if !overlaps {
                    continue;
                }
                if !seen.insert((*server_id, index)) {
                    continue;
                }
                let lsp_link = lsp::DocumentLink {
                    range: lsp::Range {
                        start: point_to_lsp(link.range.start.to_point_utf16(&snapshot)),
                        end: point_to_lsp(link.range.end.to_point_utf16(&snapshot)),
                    },
                    target: link
                        .target
                        .as_ref()
                        .and_then(|s| lsp::Uri::from_str(s).ok()),
                    tooltip: link.tooltip.as_ref().map(|t| t.to_string()),
                    data: link.data.clone(),
                };
                to_resolve.push((*server_id, index, lsp_link));
            }
        }
        if to_resolve.is_empty() {
            return Task::ready(Vec::new());
        }

        if let Some(document_links) = self
            .lsp_data
            .get_mut(&buffer_id)
            .and_then(|data| data.document_links.as_mut())
        {
            for (server_id, index, _) in &to_resolve {
                document_links.resolving.insert((*server_id, *index));
            }
        }

        let request_timeout = ProjectSettings::get_global(cx)
            .global_lsp_settings
            .get_request_timeout();
        let query_version = snapshot.version().clone();

        cx.spawn(async move |lsp_store, cx| {
            let mut resolved = Vec::new();
            for (server_id, index, lsp_link) in &to_resolve {
                let resolve_outcome: anyhow::Result<lsp::DocumentLink> =
                    if let Some((upstream_client, project_id)) = &upstream {
                        let request = proto::ResolveDocumentLink {
                            project_id: *project_id,
                            buffer_id: buffer_id.into(),
                            language_server_id: server_id.0 as u64,
                            lsp_link: serde_json::to_vec(lsp_link).unwrap_or_default(),
                        };
                        match upstream_client.request(request).await {
                            Ok(response) => {
                                serde_json::from_slice::<lsp::DocumentLink>(&response.lsp_link)
                                    .context("deserializing resolved document link")
                            }
                            Err(e) => Err(e),
                        }
                    } else if let Some(server) = local_servers.get(server_id) {
                        server
                            .request::<DocumentLinkResolve>(lsp_link.clone(), request_timeout)
                            .await
                            .into_response()
                            .map_err(anyhow::Error::from)
                    } else {
                        continue;
                    };
                match resolve_outcome {
                    Ok(resolved_link) => resolved.push((*server_id, *index, resolved_link)),
                    Err(e) => log::warn!("Failed to resolve document link: {e:#}"),
                }
            }

            lsp_store
                .update(cx, |lsp_store, _| {
                    let Some(document_links) =
                        lsp_store.lsp_data.get_mut(&buffer_id).and_then(|data| {
                            if data.buffer_version != query_version {
                                None
                            } else {
                                data.document_links.as_mut()
                            }
                        })
                    else {
                        return Vec::new();
                    };
                    for (server_id, index, _) in &to_resolve {
                        document_links.resolving.remove(&(*server_id, *index));
                    }

                    let mut newly_resolved = Vec::new();
                    for (server_id, index, resolved_link) in resolved {
                        if let Some(links) = document_links.links.get_mut(&server_id) {
                            if let Some(link) = links.get_mut(index) {
                                link.target = resolved_link.target.map(|u| u.to_string().into());
                                if let Some(tooltip) = resolved_link.tooltip {
                                    link.tooltip = Some(tooltip.into());
                                }
                                link.data = resolved_link.data;
                                newly_resolved.push(link.clone());
                            }
                        }
                    }
                    newly_resolved
                })
                .unwrap_or_default()
        })
    }

    pub(super) async fn handle_resolve_document_link(
        lsp_store: Entity<Self>,
        envelope: TypedEnvelope<proto::ResolveDocumentLink>,
        mut cx: AsyncApp,
    ) -> anyhow::Result<proto::ResolveDocumentLinkResponse> {
        let lsp_link: lsp::DocumentLink = serde_json::from_slice(&envelope.payload.lsp_link)
            .context("deserializing document link to resolve")?;
        let server_id = LanguageServerId::from_proto(envelope.payload.language_server_id);

        let resolve_task = lsp_store.update(&mut cx, |lsp_store, cx| {
            let server = lsp_store
                .language_server_for_id(server_id)
                .with_context(|| format!("No language server {server_id}"))?;
            anyhow::ensure!(
                can_resolve_link(&server.capabilities()),
                "Server {server_id} does not advertise documentLink/resolve"
            );
            let timeout = ProjectSettings::get_global(cx)
                .global_lsp_settings
                .get_request_timeout();
            anyhow::Ok(server.request::<DocumentLinkResolve>(lsp_link, timeout))
        })?;
        let resolved = resolve_task.await.into_response()?;
        Ok(proto::ResolveDocumentLinkResponse {
            lsp_link: serde_json::to_vec(&resolved)
                .context("serializing resolved document link")?,
        })
    }
}

fn can_resolve_link(capabilities: &lsp::ServerCapabilities) -> bool {
    capabilities
        .document_link_provider
        .as_ref()
        .and_then(|opts| opts.resolve_provider)
        .unwrap_or(false)
}
