use std::ops::Range;

use collections::HashMap;
use futures::future::join_all;
use gpui::{App, Entity, Task};
use itertools::Itertools;
use language::Buffer;
use project::lsp_store::LspDocumentLink;
use settings::Settings;
use text::{Anchor, BufferId};
use ui::Context;

use crate::{Editor, LSP_REQUEST_DEBOUNCE_TIMEOUT, editor_settings::EditorSettings};

pub(super) struct LspDocumentLinks {
    pub(super) enabled: bool,
    pub(super) per_buffer: HashMap<BufferId, Vec<LspDocumentLink>>,
    pub(super) refresh_task: Task<()>,
    pub(super) resolve_task: Task<()>,
}

impl LspDocumentLinks {
    pub(super) fn new(cx: &App) -> Self {
        Self {
            enabled: EditorSettings::get_global(cx).lsp_document_links,
            per_buffer: HashMap::default(),
            refresh_task: Task::ready(()),
            resolve_task: Task::ready(()),
        }
    }
}

impl Editor {
    pub(super) fn refresh_document_links(
        &mut self,
        for_buffer: Option<BufferId>,
        cx: &mut Context<Self>,
    ) {
        if !self.lsp_data_enabled() || !self.lsp_document_links.enabled {
            return;
        }
        let Some(project) = self.project.clone() else {
            return;
        };

        let buffers_to_query = self
            .visible_buffers(cx)
            .into_iter()
            .filter(|buffer| self.is_lsp_relevant(buffer.read(cx).file(), cx))
            .chain(for_buffer.and_then(|id| self.buffer.read(cx).buffer(id)))
            .filter(|buffer| {
                let id = buffer.read(cx).remote_id();
                for_buffer.is_none_or(|target| target == id)
                    && self.registered_buffers.contains_key(&id)
            })
            .unique_by(|buffer| buffer.read(cx).remote_id())
            .collect::<Vec<_>>();

        if buffers_to_query.is_empty() {
            return;
        }

        self.lsp_document_links.refresh_task = cx.spawn(async move |editor, cx| {
            cx.background_executor()
                .timer(LSP_REQUEST_DEBOUNCE_TIMEOUT)
                .await;

            let Some(tasks) = editor
                .update(cx, |_, cx| {
                    project.read(cx).lsp_store().update(cx, |lsp_store, cx| {
                        buffers_to_query
                            .into_iter()
                            .map(|buffer| {
                                let buffer_id = buffer.read(cx).remote_id();
                                let task = lsp_store.fetch_document_links(&buffer, cx);
                                async move { (buffer_id, task.await) }
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .ok()
            else {
                return;
            };

            let results = join_all(tasks).await;
            if results.is_empty() {
                return;
            }

            editor
                .update(cx, |editor, cx| {
                    let mut any_updated = false;
                    for (buffer_id, maybe_links) in results {
                        // `None` means "skipped or errored" — keep the
                        // existing cache to avoid link underline blinking.
                        let Some(links) = maybe_links else {
                            continue;
                        };
                        any_updated = true;
                        if links.is_empty() {
                            editor.lsp_document_links.per_buffer.remove(&buffer_id);
                        } else {
                            editor
                                .lsp_document_links
                                .per_buffer
                                .insert(buffer_id, links);
                        }
                    }
                    if any_updated {
                        editor.resolve_visible_document_links(cx);
                        cx.notify();
                    }
                })
                .ok();
        });
    }

    pub(super) fn resolve_visible_document_links(&mut self, cx: &mut Context<Self>) {
        if !self.lsp_data_enabled() || !self.lsp_document_links.enabled {
            return;
        }
        let Some(project) = self.project.clone() else {
            return;
        };

        let resolve_tasks = self
            .visible_buffer_ranges(cx)
            .into_iter()
            .filter_map(|(snapshot, visible_range, _)| {
                let buffer_id = snapshot.remote_id();
                let buffer = self.buffer.read(cx).buffer(buffer_id)?;
                let visible_anchor_range = snapshot.anchor_before(visible_range.start)
                    ..snapshot.anchor_after(visible_range.end);
                let task = project.update(cx, |project, cx| {
                    project.lsp_store().update(cx, |lsp_store, cx| {
                        lsp_store.resolve_document_links(&buffer, &[visible_anchor_range], cx)
                    })
                });
                Some((buffer_id, task))
            })
            .collect::<Vec<_>>();
        if resolve_tasks.is_empty() {
            return;
        }

        let buffer_ids = resolve_tasks.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        self.lsp_document_links.resolve_task = cx.spawn(async move |editor, cx| {
            let resolved = join_all(
                resolve_tasks
                    .into_iter()
                    .map(|(buffer_id, task)| async move { (buffer_id, task.await) }),
            )
            .await;

            let has_newly_resolved = resolved.iter().any(|(_, links)| !links.is_empty());
            if !has_newly_resolved {
                return;
            }

            editor
                .update(cx, |editor, cx| {
                    let lsp_store = project.read(cx).lsp_store().read(cx);
                    for buffer_id in &buffer_ids {
                        if let Some(all_links) = lsp_store.document_links_for_buffer(*buffer_id) {
                            if all_links.is_empty() {
                                editor.lsp_document_links.per_buffer.remove(buffer_id);
                            } else {
                                editor
                                    .lsp_document_links
                                    .per_buffer
                                    .insert(*buffer_id, all_links);
                            }
                        }
                    }
                    cx.notify();
                })
                .ok();
        });
    }

    pub(super) fn document_link_at(
        &self,
        buffer_id: BufferId,
        position: &text::Anchor,
        snapshot: &language::BufferSnapshot,
    ) -> Option<&LspDocumentLink> {
        self.lsp_document_links
            .per_buffer
            .get(&buffer_id)?
            .iter()
            .find(|link| {
                link.range.start.cmp(position, snapshot).is_le()
                    && link.range.end.cmp(position, snapshot).is_ge()
            })
    }

    pub(super) fn resolve_document_link(
        &mut self,
        buffer: Entity<Buffer>,
        range: Range<Anchor>,
        cx: &mut Context<Self>,
    ) -> Task<Option<LspDocumentLink>> {
        let buffer_id = buffer.read(cx).remote_id();
        let cached = self
            .lsp_document_links
            .per_buffer
            .get(&buffer_id)
            .and_then(|links| links.iter().find(|link| link.range == range))
            .cloned();

        let Some(project) = self.project.clone() else {
            return Task::ready(cached);
        };
        let resolve_task = project.update(cx, |project, cx| {
            project.lsp_store().update(cx, |lsp_store, cx| {
                lsp_store.resolve_document_links(&buffer, &[range.clone()], cx)
            })
        });

        cx.spawn(async move |editor, cx| {
            let mut resolved = resolve_task.await;
            let matched = resolved
                .iter()
                .position(|link| link.range == range)
                .map(|index| resolved.swap_remove(index));

            if let Some(link) = &matched {
                editor
                    .update(cx, |editor, cx| {
                        if let Some(links) =
                            editor.lsp_document_links.per_buffer.get_mut(&buffer_id)
                        {
                            if let Some(slot) = links.iter_mut().find(|cached| {
                                cached.server_id == link.server_id && cached.range == link.range
                            }) {
                                *slot = link.clone();
                            }
                        }
                        cx.notify();
                    })
                    .ok();
            }

            matched.or(cached)
        })
    }
}
