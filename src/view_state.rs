use crate::ui::{AppWindow, DirectoryRow, FileRow, TerminalRun};
use slint::{Model, ModelRc, SharedString};
use std::collections::HashMap;

const MAX_VIEWS: usize = 32;

#[derive(Default)]
pub(crate) struct SessionViews {
    active_id: String,
    views: HashMap<String, SessionView>,
}

impl SessionViews {
    pub(crate) fn activate(&mut self, c_ui: &AppWindow, c_id: &str) -> bool {
        if self.active_id == c_id {
            return false;
        }
        self.retain(c_ui);
        let z_sessions = c_ui.get_sessions();
        // 关闭后的标签不再保存, 避免把删除的视图重新放回缓存.
        if !self.active_id.is_empty()
            && z_sessions
                .iter()
                .any(|c| c.id.as_str() == self.active_id.as_str())
            && (self.views.contains_key(&self.active_id) || self.views.len() < MAX_VIEWS)
        {
            self.views
                .insert(self.active_id.clone(), SessionView::capture(c_ui));
        }
        self.active_id = c_id.to_owned();
        c_ui.set_active_session(c_id.into());
        c_ui.invoke_reveal_session_tab(z_sessions.iter().position(|c| c.id.as_str() == c_id).map(|c| c as i32).unwrap_or(-1));
        let z_row = z_sessions.iter().find(|c| c.id.as_str() == c_id);
        c_ui.set_active_session_empty(z_row.as_ref().is_none_or(|c| c.empty));
        c_ui.set_active_state(z_row.as_ref().map(|c| c.state).unwrap_or(0));
        // 连接 ID 与失败原因随下一次状态刷新填入, 切换时先清空, 避免"编辑连接"指向上一个标签.
        c_ui.set_active_connection_id(SharedString::default());
        c_ui.set_active_error(SharedString::default());
        c_ui.set_selected_terminal_text(SharedString::default());
        c_ui.set_selection_spans(ModelRc::default());
        c_ui.set_selection_dragging(false);
        c_ui.set_tree_loading(SharedString::default());
        c_ui.set_file_loading_path(SharedString::default());
        match self.views.get(c_id) {
            Some(z_view) => z_view.restore(c_ui),
            None => SessionView {
                browse_path: ".".into(),
                ..SessionView::default()
            }
            .restore(c_ui),
        }
        true
    }

    pub(crate) fn retain(&mut self, c_ui: &AppWindow) {
        let z_sessions = c_ui.get_sessions();
        self.views
            .retain(|c_id, _| z_sessions.iter().any(|c| c.id.as_str() == c_id.as_str()));
    }

    // c_discard: 会话已换成新连接 (同一标签重连), 旧列表与目录树不能再显示或操作, 一并清空; 否则读取完成前保留旧列表.
    pub(crate) fn invalidate_files(&mut self, c_ui: &AppWindow, c_id: &str, c_discard: bool) {
        crate::ui::invalidate_file_cache(c_ui, c_id);
        if c_id == self.active_id {
            c_ui.set_remote_listed_session(SharedString::default());
            if c_discard {
                c_ui.set_files(ModelRc::default());
                c_ui.set_directory_tree(ModelRc::default());
            }
        } else if let Some(z_view) = self.views.get_mut(c_id) {
            if c_discard {
                z_view.files = ModelRc::default();
                z_view.directory_tree = ModelRc::default();
            }
            z_view.remote_listed_session = SharedString::default();
            z_view.remote_ready = false;
            z_view.selected_remote = SharedString::default();
            z_view.selected_files_count = 0;
            z_view.selected_file_paths = SharedString::default();
        }
    }
}

#[derive(Default)]
struct SessionView {
    terminal_text: SharedString,
    terminal_runs: ModelRc<TerminalRun>,
    terminal_display_cols: i32,
    terminal_display_rows: i32,
    terminal_wrapped_rows: ModelRc<i32>,
    terminal_history_lines: i32,
    terminal_view_offset: i32,
    cursor_row: i32,
    cursor_col: i32,
    alternate_screen: bool,
    terminal_scroll_offset: i32,
    bracketed_paste: bool,
    application_cursor: bool,
    modified_keys: i32,
    hide_cursor: bool,
    cursor_shape: i32,
    mouse_mode: i32,
    files: ModelRc<FileRow>,
    directory_tree: ModelRc<DirectoryRow>,
    browse_path: SharedString,
    remote_listed_session: SharedString,
    file_time_label: SharedString,
    remote_ready: bool,
    selected_remote: SharedString,
    selected_files_count: i32,
    selected_file_paths: SharedString,
    file_scroll_y: f32,
    tree_scroll_y: f32,
    network_interface_name: SharedString,
    network_interface_index: i32,
    command_text: SharedString,
}

impl SessionView {
    fn capture(c_ui: &AppWindow) -> Self {
        Self {
            terminal_text: c_ui.get_terminal_text(),
            terminal_runs: c_ui.get_terminal_runs(),
            terminal_display_cols: c_ui.get_terminal_display_cols(),
            terminal_display_rows: c_ui.get_terminal_display_rows(),
            terminal_wrapped_rows: c_ui.get_terminal_wrapped_rows(),
            terminal_history_lines: c_ui.get_terminal_history_lines(),
            terminal_view_offset: c_ui.get_terminal_view_offset(),
            cursor_row: c_ui.get_cursor_row(),
            cursor_col: c_ui.get_cursor_col(),
            alternate_screen: c_ui.get_alternate_screen(),
            terminal_scroll_offset: c_ui.get_terminal_scroll_offset(),
            bracketed_paste: c_ui.get_bracketed_paste(),
            application_cursor: c_ui.get_application_cursor(),
            modified_keys: c_ui.get_modified_keys(),
            hide_cursor: c_ui.get_hide_cursor(),
            cursor_shape: c_ui.get_cursor_shape(),
            mouse_mode: c_ui.get_mouse_mode(),
            files: c_ui.get_files(),
            directory_tree: c_ui.get_directory_tree(),
            browse_path: c_ui.get_browse_path(),
            // 切换会丢弃旧目录请求, 未完成的列表在恢复后需要重新读取.
            remote_listed_session: if c_ui.get_remote_ready() {
                c_ui.get_remote_listed_session()
            } else {
                SharedString::default()
            },
            remote_ready: c_ui.get_remote_ready(),
            file_time_label: c_ui.get_file_time_label(),
            selected_remote: c_ui.get_selected_remote(),
            selected_files_count: c_ui.get_selected_files_count(),
            selected_file_paths: c_ui.get_selected_file_paths(),
            file_scroll_y: c_ui.get_file_scroll_y(),
            tree_scroll_y: c_ui.get_tree_scroll_y(),
            network_interface_name: c_ui.get_network_interface_name(),
            network_interface_index: c_ui.get_network_interface_index(),
            command_text: c_ui.get_command_text(),
        }
    }

    fn restore(&self, c_ui: &AppWindow) {
        c_ui.set_terminal_text(self.terminal_text.clone());
        c_ui.set_terminal_display_cols(self.terminal_display_cols);
        c_ui.set_terminal_display_rows(self.terminal_display_rows);
        c_ui.set_terminal_wrapped_rows(self.terminal_wrapped_rows.clone());
        c_ui.set_terminal_history_lines(self.terminal_history_lines);
        c_ui.set_terminal_view_offset(self.terminal_view_offset);
        c_ui.set_cursor_row(self.cursor_row);
        c_ui.set_cursor_col(self.cursor_col);
        c_ui.set_alternate_screen(self.alternate_screen);
        c_ui.set_terminal_scroll_offset(self.terminal_scroll_offset);
        c_ui.set_bracketed_paste(self.bracketed_paste);
        c_ui.set_application_cursor(self.application_cursor);
        c_ui.set_modified_keys(self.modified_keys);
        c_ui.set_hide_cursor(self.hide_cursor);
        c_ui.set_cursor_shape(self.cursor_shape);
        c_ui.set_mouse_mode(self.mouse_mode);
        c_ui.set_terminal_runs(self.terminal_runs.clone());
        c_ui.set_files(self.files.clone());
        c_ui.set_directory_tree(self.directory_tree.clone());
        c_ui.set_browse_path(self.browse_path.clone());
        c_ui.set_remote_listed_session(self.remote_listed_session.clone());
        c_ui.set_file_time_label(if self.file_time_label.is_empty() { "修改时间".into() } else { self.file_time_label.clone() });
        c_ui.set_remote_ready(self.remote_ready);
        c_ui.set_selected_remote(self.selected_remote.clone());
        c_ui.set_selected_files_count(self.selected_files_count);
        c_ui.set_selected_file_paths(self.selected_file_paths.clone());
        c_ui.set_file_scroll_y(self.file_scroll_y);
        c_ui.set_tree_scroll_y(self.tree_scroll_y);
        c_ui.set_network_interface_name(self.network_interface_name.clone());
        c_ui.set_network_interface_index(self.network_interface_index);
        c_ui.set_command_text(self.command_text.clone());
    }
}
