１．構成
　　以下にソリューション構成を記す。
　　【ServerAPI】
　　TransitionGraphAPIServer（csproj、親）
　　┣TransionGraphAPI（sln、子）
　　┣TransitionGraphEdit（csproj、子）
　　┣TransitionGraphDBAccess（csproj、子）
　　┣GlobalCache（csproj、共通）
　　┣LogOutPut（csproj、共通）
　　┣PortSetting（csproj、共通）
　　┣Protos（csproj、共通）
　　┣Shared（csproj、共通）
　　┣TransitionGraphDtos（csproj、共通）
　　┗TransitionGraphMapperly（csproj、共通）
　　
　　【ClientAPI】
　　TransitionGraphAPIClient（sln、親）
　　┣CTransionGraphAPI（csproj、子）
　　┣LogOutPut（csproj、共通）
　　┣PortSetting（csproj、共通）
　　┗TransitionGraphDtos（csproj、共通）

２．設定
　　PortSetting.PortSetting.properties
　　以下の内容を記載している。
　　(1)サーバのIP
　　(2)サーバ・クライアント間gRPCの使用ポート
　　(3)サーバ内使用ポート
　　(4)クライアント内使用ポート

　　LogOutPut.properties
　　以下の内容を記載している。
　　(1)ログ出力先
　　(2)ログファイル最大サイズ
　　(3)ログ出力レベル

　　TransitionGraphDBAccess内appsetting.json
　　以下の内容を記載している。
　　(1)使用DB
　　(2)接続タイムアウト時間（秒）
　　(3)接続文字列（サーバ接続先、3種類）

　　TransitionGraphDataEdit内appsetting.json
　　以下の内容を記載している。
　　(1)DummyDLLの使用可否
　　(2)本番用DLL使用のためのルートDir

　　TransitionGraphAPIServer内appsetting.json
　　(1)子のexeの場所（絶対・相対パスどちらもOK）

　　TransitionGraphAlignment内appsetting.json
　　以下の内容を記載している。
　　(1)DummyDLLの使用可否
