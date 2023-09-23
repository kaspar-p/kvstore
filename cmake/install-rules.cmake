install(
    TARGETS kvstore_exe
    RUNTIME COMPONENT kvstore_Runtime
)

if(PROJECT_IS_TOP_LEVEL)
  include(CPack)
endif()
