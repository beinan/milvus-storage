#[cxx::bridge(namespace = "milvus_storage::source_context::ffi")]
pub mod source_context_ffi {
    #[derive(Debug, PartialEq, Eq)]
    enum SourcePlanFailure {
        None,
        Denied,
        Expired,
        CredentialTransient,
        Cancelled,
        InvalidCredentialResponse,
        IoUnexpected,
        IoUnsupported,
        IoConfigInvalid,
        IoNotFound,
        IoPermissionDenied,
        IoIsADirectory,
        IoNotADirectory,
        IoAlreadyExists,
        IoRateLimited,
        IoIsSameFile,
        IoConditionNotMatch,
        IoRangeNotSatisfied,
        IoUnknown,
        OtherPlannerFailure,
    }

    // Failures contain only safe, typed metadata. Never stringify an upstream
    // error across CXX: it can contain a signed URL or lose the provider status.
    struct IcebergSourcePlanResult {
        files: Vec<SourceIcebergFileInfo>,
        failure: SourcePlanFailure,
        io_temporary: bool,
        io_persistent: bool,
    }

    struct SourceIcebergFileInfo {
        data_file_path: String,
        record_count: u64,
        num_deleted_rows: u64,
        delete_metadata_json: Vec<u8>,
    }

    struct ResolvedSource {
        status: u32,
        resource_uri: String,
        scope_root_uri: String,
        root_index: u64,
        operation: u32,
    }

    // Do not derive Debug: this value contains a credential.
    struct SourceCredential {
        status: u32,
        sas_token: String,
        expires_at_unix_ms: i64,
    }

    unsafe extern "C++" {
        include!("source_context_bridge.h");
        type SourceContextBridge;
        fn identity(self: &SourceContextBridge) -> u64;
        fn now_unix_ms(self: &SourceContextBridge) -> i64;
        fn check_state(self: &SourceContextBridge) -> u32;
        fn request_timeout_ms(self: &SourceContextBridge) -> i64;
        fn retain(self: &SourceContextBridge) -> Result<SharedPtr<SourceContextBridge>>;
        fn resolve(self: &SourceContextBridge, resource_uri: &str) -> Result<ResolvedSource>;
        fn resolve_http(self: &SourceContextBridge, method: &str, uri: &str) -> Result<ResolvedSource>;
        fn fetch(self: &SourceContextBridge, resource_uri: &str, operation: u32) -> Result<SourceCredential>;
    }
}

