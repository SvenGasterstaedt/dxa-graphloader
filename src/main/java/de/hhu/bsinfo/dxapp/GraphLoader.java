package de.hhu.bsinfo.dxapp;

/*
@SuppressWarnings("Duplicates")
class GraphLoader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private final String format;
    private final String[] files;
    private List<Short> peers;
    private List<Long> filechunks_ids;
    private final GraphLoaderApp dxramServiceAccessor;
    private final ChunkService chunkService;
    private final FunctionService functionService;


    protected GraphLoader(final GraphLoaderApp dxramServiceAccessor, final String format, final String[] files, final List<Short> peers) throws Exception {
        super();
        this.format = format.toUpperCase();
        this.filechunks_ids = new ArrayList<>();
        this.files = files;
        this.peers = peers;
        this.dxramServiceAccessor = dxramServiceAccessor;
        chunkService = dxramServiceAccessor.getService(ChunkService.class);
        functionService = dxramServiceAccessor.getService(FunctionService.class);

        if (!SupportedFormats.isSupported(format)) {
            LOGGER.error(this.format + " is no not supported!");
            LOGGER.info("List of supported formats:");
            for (String f : SupportedFormats.supportedFormats()) {
                LOGGER.info(f);
            }
            throw new Exception("GraphLoader terminated!");
        }

        for (String file : files) {
            if (!Files.isRegularFile(Paths.get(file))) {
                LOGGER.error(file + " is no regular file!");
                throw new Exception("GraphLoader terminated!");
            }
        }
    }

    protected void execute() {

        GraphFormat graphFormat = SupportedFormats.getFormat(format, files);
        FileChunkCreator chunkCreator;
        if (graphFormat != null) {
            chunkCreator = graphFormat.getFileChunkCreator();
            while (chunkCreator.hasRemaining()) {
                for (short p : peers) {
                    long c_id = ChunkID.INVALID_ID;
                    FileChunk fileChunk = chunkCreator.getNextChunk();
                    chunkService.create().create(p, fileChunk);
                    chunkService.put().put(fileChunk);
                    filechunks_ids.add(fileChunk.getID());

                    DistributableFunction function = new RemoteJob();
                    FunctionService.Status status = functionService.registerFunction(p, RemoteJob.NAME, function);
                    ParameterList params, result;
                    params = new ParameterList(new String[]{"17", "25"});
                    result = functionService.executeFunctionSync(p, RemoteJob.NAME, params);

                    System.out.println(result.get(0));

                    if (!chunkCreator.hasRemaining()) {
                        break;
                    }
                }
            }
            filechunks_ids.stream().forEach(LOGGER::debug);
        }
    }
}*/
