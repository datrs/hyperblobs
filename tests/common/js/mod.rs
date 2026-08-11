use rusty_nodejs_repl::{
    integration_utils::{git_root, run_make},
    join_paths,
};

use std::{path::PathBuf, sync::LazyLock};

pub static REL_PATH_TO_NODE_MODULES: &str = "./tests/common/js/node_modules";
pub static REL_PATH_TO_JS_DIR: &str = "./tests/common/js";

pub static REQUIRE_JS: LazyLock<()> = LazyLock::new(|| {
    let _ = run_make(REL_PATH_TO_JS_DIR, "node_modules").expect("Failed to setup node_modules");
});

pub fn path_to_node_modules() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let p = join_paths!(git_root()?, &REL_PATH_TO_NODE_MODULES);
    Ok(p.into())
}
