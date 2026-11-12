use crate::ui::AppWindow;
use slint::{
    ComponentHandle,
    winit_030::{EventResult, WinitWindowAccessor, winit},
};
use std::{cell::RefCell, path::PathBuf, rc::Rc, time::Duration};

struct DropBatch {
    paths: Vec<PathBuf>,
    session: String,
    directory: String,
    position: (f32, f32),
}

pub(crate) fn install(c_ui: &AppWindow) {
    // 小屏或高缩放时默认窗口可能比屏幕还大 (标题栏或底部落在屏幕外): 窗口创建后发现放不下就最大化, 正好占满工作区.
    // 高度给任务栏、菜单栏约留出屏幕的 1/12.
    let z_weak = c_ui.as_weak();
    slint::Timer::single_shot(Duration::ZERO, move || {
        let Some(z_ui) = z_weak.upgrade() else { return; };
        z_ui.window().with_winit_window(|c_window| {
            // 恢复的位置不在任何显示器上 (显示器已拔掉或分辨率变了) 时, 移到主显示器左上角附近, 标题栏保持可见.
            if let Ok(l_position) = c_window.outer_position() {
                let l_visible = c_window.available_monitors().any(|c| {
                    let (l_origin, l_size) = (c.position(), c.size());
                    l_position.x >= l_origin.x - 50 && l_position.x < l_origin.x + l_size.width as i32 - 100
                        && l_position.y >= l_origin.y - 10 && l_position.y < l_origin.y + l_size.height as i32 - 60
                });
                if !l_visible {
                    let l_origin = c_window.primary_monitor().or_else(|| c_window.current_monitor()).map(|c| c.position()).unwrap_or_default();
                    c_window.set_outer_position(winit::dpi::PhysicalPosition::new(l_origin.x + 60, l_origin.y + 60));
                }
            }
            let Some(l_screen) = c_window.current_monitor().map(|c| c.size()) else { return; };
            let l_window = c_window.outer_size();
            if l_window.width > l_screen.width || l_window.height + l_screen.height / 12 > l_screen.height {
                c_window.set_maximized(true);
            }
        });
    });
    let z_weak = c_ui.as_weak();
    let z_pending = Rc::new(RefCell::new(None::<DropBatch>));
    let z_window = c_ui.window();
    #[cfg(windows)]
    let z_minimized = std::cell::Cell::new(false);
    z_window.on_winit_window_event(move |c_window, c_event| {
        // Windows: 最小化期间绘制的内容不会留在窗口上, 恢复后软件渲染器仍认为缓冲区有效, 只补画变化的部分,
        // 窗口因此只显示一半或几块 (最大化时恢复动画更长, 更常见). 恢复、重新可见或获得焦点时让整窗重绘,
        // 动画期间画的帧可能再丢, 0.3 秒和 1 秒后各补一次. 启用 Skia 渲染时不走此路径, 仅显卡不可用退回软件渲染时需要.
        #[cfg(windows)]
        {
            let z_restore = match c_event {
                winit::event::WindowEvent::Resized(l_size) => {
                    let l_zero = l_size.width == 0 || l_size.height == 0;
                    z_minimized.replace(l_zero) && !l_zero
                }
                winit::event::WindowEvent::Occluded(false) | winit::event::WindowEvent::Focused(true) => true,
                _ => false,
            };
            if z_restore {
                if let Some(z_ui) = z_weak.upgrade() {
                    z_ui.set_repaint_flip(!z_ui.get_repaint_flip());
                    for l_delay in [300, 1000] {
                        let l_weak = z_ui.as_weak();
                        slint::Timer::single_shot(Duration::from_millis(l_delay), move || {
                            if let Some(l_ui) = l_weak.upgrade() {
                                l_ui.set_repaint_flip(!l_ui.get_repaint_flip());
                            }
                        });
                    }
                }
            }
        }
        if let winit::event::WindowEvent::ModifiersChanged(z_modifiers) = c_event {
            if let Some(z_ui) = z_weak.upgrade() {
                let z_state = z_modifiers.state();
                let z_control = if cfg!(target_os = "macos") {
                    z_state.super_key()
                } else {
                    z_state.control_key()
                };
                let z_meta = if cfg!(target_os = "macos") {
                    z_state.control_key()
                } else {
                    z_state.super_key()
                };
                // flagsChanged 可能没有对应的按键事件, 同步 Slint 状态让标准输入框也能使用快捷键.
                for (z_key, z_previous, z_pressed) in [
                    (slint::platform::Key::Control, z_ui.get_native_control(), z_control),
                    (slint::platform::Key::Meta, z_ui.get_native_meta(), z_meta),
                    (slint::platform::Key::Alt, z_ui.get_native_alt(), z_state.alt_key()),
                    (slint::platform::Key::Shift, z_ui.get_native_shift(), z_state.shift_key()),
                ] {
                    if z_previous != z_pressed {
                        c_window.dispatch_event(if z_pressed {
                            slint::platform::WindowEvent::KeyPressed { text: z_key.into() }
                        } else {
                            slint::platform::WindowEvent::KeyReleased { text: z_key.into() }
                        });
                    }
                }
                z_ui.set_native_control(z_control);
                z_ui.set_native_meta(z_meta);
                z_ui.set_native_alt(z_state.alt_key());
                z_ui.set_native_shift(z_state.shift_key());
                z_ui.set_native_modifiers_valid(true);
            }
            return EventResult::Propagate;
        }
        // 未启用原生菜单栏, macOS 的 ⌘Q 需要自行处理; 放在窗口事件钩子里, 焦点在任何输入框都生效.
        #[cfg(target_os = "macos")]
        if let winit::event::WindowEvent::KeyboardInput { event, .. } = c_event {
            if event.state.is_pressed()
                && matches!(&event.logical_key, winit::keyboard::Key::Character(c) if c.eq_ignore_ascii_case("q"))
                && z_weak.upgrade().is_some_and(|c| c.get_native_control())
            {
                let _ = slint::quit_event_loop();
                return EventResult::PreventDefault;
            }
        }
        if matches!(c_event, winit::event::WindowEvent::Focused(false)) {
            if let Some(z_ui) = z_weak.upgrade() {
                for z_key in [slint::platform::Key::Control, slint::platform::Key::Meta, slint::platform::Key::Alt, slint::platform::Key::Shift] {
                    c_window.dispatch_event(slint::platform::WindowEvent::KeyReleased { text: z_key.into() });
                }
                z_ui.set_native_control(false);
                z_ui.set_native_meta(false);
                z_ui.set_native_alt(false);
                z_ui.set_native_shift(false);
                z_ui.set_native_modifiers_valid(false);
            }
        }
        let winit::event::WindowEvent::DroppedFile(z_path) = c_event else {
            return EventResult::Propagate;
        };
        let Some(z_ui) = z_weak.upgrade() else {
            return EventResult::PreventDefault;
        };
        let Some(z_position) = c_window.with_winit_window(drop_position).flatten() else {
            z_ui.set_status("无法确定拖放位置, 请使用右键上传".into());
            return EventResult::PreventDefault;
        };
        if !z_ui.invoke_native_drop_target(z_position.0, z_position.1) {
            z_ui.set_status("请将文件拖入已连接的文件管理区域".into());
            return EventResult::PreventDefault;
        }
        let z_schedule = z_pending.borrow().is_none();
        {
            let mut z_pending = z_pending.borrow_mut();
            let z_batch = z_pending.get_or_insert_with(|| DropBatch {
                paths: Vec::with_capacity(17),
                session: z_ui.get_active_session().to_string(),
                directory: z_ui.get_browse_path().to_string(),
                position: z_position,
            });
            // 合并同一批系统事件, 第17项触发既有的整批数量限制.
            if z_batch.paths.len() < 17 && !z_batch.paths.contains(&z_path) {
                z_batch.paths.push(z_path.clone());
            }
        }
        if z_schedule {
            let z_weak = z_weak.clone();
            let z_pending = z_pending.clone();
            slint::Timer::single_shot(Duration::from_millis(1), move || {
                let Some(z_batch) = z_pending.borrow_mut().take() else {
                    return;
                };
                let Some(z_ui) = z_weak.upgrade() else {
                    return;
                };
                if z_ui.get_active_session().as_str() != z_batch.session
                    || z_ui.get_browse_path().as_str() != z_batch.directory
                    || !z_ui.invoke_native_drop_target(z_batch.position.0, z_batch.position.1)
                {
                    z_ui.set_status("上传已取消, 目标目录或窗口状态已变化".into());
                    return;
                }
                let mut z_data = slint::DataTransfer::default();
                z_data.set_file_paths(z_batch.paths);
                z_ui.invoke_drop_files(z_data);
            });
        }
        EventResult::PreventDefault
    });
}

#[cfg(target_os = "macos")]
fn drop_position(c_window: &winit::window::Window) -> Option<(f32, f32)> {
    use std::ffi::{c_char, c_void};
    use winit::raw_window_handle::{HasWindowHandle, RawWindowHandle};

    #[repr(C)]
    struct Point {
        x: f64,
        y: f64,
    }

    #[link(name = "objc")]
    unsafe extern "C" {
        #[link_name = "objc_msgSend"]
        fn objc_msg_send();
        #[link_name = "sel_registerName"]
        fn sel_register_name(c_name: *const c_char) -> *const c_void;
    }

    let RawWindowHandle::AppKit(z_handle) = c_window.window_handle().ok()?.as_raw() else {
        return None;
    };
    // 只在事件主线程借用存活的 NSView, Winit 视图以左上角为原点.
    // macOS arm64/x86_64 的 NSPoint 返回 ABI 与下列 objc_msgSend 签名一致.
    unsafe {
        let z_send_window: unsafe extern "C" fn(*mut c_void, *const c_void) -> *mut c_void =
            std::mem::transmute(objc_msg_send as unsafe extern "C" fn());
        let z_send_point: unsafe extern "C" fn(*mut c_void, *const c_void) -> Point =
            std::mem::transmute(objc_msg_send as unsafe extern "C" fn());
        let z_convert_point: unsafe extern "C" fn(
            *mut c_void,
            *const c_void,
            Point,
            *mut c_void,
        ) -> Point = std::mem::transmute(objc_msg_send as unsafe extern "C" fn());
        let z_view = z_handle.ns_view.as_ptr();
        let z_window = z_send_window(z_view, sel_register_name(c"window".as_ptr()));
        if z_window.is_null() {
            return None;
        }
        let z_point = z_send_point(
            z_window,
            sel_register_name(c"mouseLocationOutsideOfEventStream".as_ptr()),
        );
        let z_local = z_convert_point(
            z_view,
            sel_register_name(c"convertPoint:fromView:".as_ptr()),
            z_point,
            std::ptr::null_mut(),
        );
        Some((z_local.x as f32, z_local.y as f32))
    }
}

#[cfg(target_os = "windows")]
fn drop_position(c_window: &winit::window::Window) -> Option<(f32, f32)> {
    use std::ffi::c_void;
    use winit::raw_window_handle::{HasWindowHandle, RawWindowHandle};

    #[repr(C)]
    struct Point {
        x: i32,
        y: i32,
    }
    #[link(name = "user32")]
    unsafe extern "system" {
        fn GetCursorPos(c_point: *mut Point) -> i32;
        fn ScreenToClient(c_window: *mut c_void, c_point: *mut Point) -> i32;
    }
    let RawWindowHandle::Win32(z_handle) = c_window.window_handle().ok()?.as_raw() else {
        return None;
    };
    let mut z_point = Point { x: 0, y: 0 };
    // 事件线程内借用有效 HWND, 屏幕像素转换为 Slint 逻辑像素.
    unsafe {
        if GetCursorPos(&mut z_point) == 0
            || ScreenToClient(z_handle.hwnd.get() as *mut c_void, &mut z_point) == 0
        {
            return None;
        }
    }
    let z_scale = c_window.scale_factor() as f32;
    Some((z_point.x as f32 / z_scale, z_point.y as f32 / z_scale))
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn drop_position(_: &winit::window::Window) -> Option<(f32, f32)> {
    None
}
