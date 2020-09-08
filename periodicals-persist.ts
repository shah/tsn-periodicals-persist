import * as pipe from "@shah/ts-pipe";
import * as pd from "@shah/tsn-periodicals";
import * as fs from "fs";
import * as path from "path";
import { v5 as uuid } from "uuid";

export type UUID = string;

export interface PersistProperties {
    [name: string]: any;
}

export interface PersistPropsTransformContext<T> {
    readonly persist: PersistProperties;
    readonly source: T;
}

export interface PersistPropsTransformer extends pipe.PipeUnionSync<PersistPropsTransformContext<any>, PersistProperties> {
}

export interface TabularColumnDefn {
    delimitedHeader(): string;
    delimitedContent(pp: PersistProperties): string;
}

export class GuessColumnDefn {
    constructor(readonly name: string, readonly guessedFrom: PersistProperties) {
    }

    delimitedHeader(): string {
        return this.name;
    }

    delimitedContent(pp: PersistProperties): string {
        const value = pp[this.name];
        return this.name == "id" || this.name.endsWith("_id")
            ? value
            : JSON.stringify(value);
    }
}

export interface TabularWriterOptions {
    readonly destPath: string;
    readonly fileName: string;
    readonly parentUuidNamespace: string;
    readonly ppTransform?: PersistPropsTransformer;
    readonly schema?: TabularColumnDefn[];
}

export class TabularWriter<T> {
    readonly columnDelim = ",";
    readonly recordDelim = "\n";
    readonly destPath: string;
    readonly fileName: string;
    readonly pkNamespace: UUID;
    readonly schema: TabularColumnDefn[];
    readonly ppTransform?: PersistPropsTransformer;
    readonly csvStream: fs.WriteStream;
    protected rowIndex: number = 0;

    constructor({ destPath, fileName, parentUuidNamespace, ppTransform, schema }: TabularWriterOptions) {
        this.destPath = destPath;
        this.fileName = fileName;
        this.csvStream = fs.createWriteStream(path.join(destPath, fileName));
        this.schema = schema || [];
        this.ppTransform = ppTransform;
        this.pkNamespace = uuid(fileName, parentUuidNamespace);
    }

    createId(name: string): UUID {
        return uuid(name, this.pkNamespace);
    }

    close(): void {
        this.csvStream.close();
    }

    guessSchema(guessFrom: PersistProperties): void {
        if (this.schema.length == 0) {
            for (const name of Object.keys(guessFrom)) {
                this.schema.push(new GuessColumnDefn(name, guessFrom));
            }
        }
    }

    writeDelimitedHeader(guess: PersistProperties): void {
        this.guessSchema(guess);
        const headers: string[] = [];
        for (const column of this.schema) {
            headers.push(column.delimitedHeader());
        }
        this.csvStream.write(headers.join(this.columnDelim));
    }

    write(ctx: PersistPropsTransformContext<T>): boolean {
        let persist = ctx.persist;
        if (this.ppTransform) {
            persist = this.ppTransform.flow(ctx, persist);
        }
        if (persist) {
            if (this.rowIndex == 0) {
                this.writeDelimitedHeader(persist);
            }
            const content: string[] = [];
            for (const column of this.schema) {
                content.push(column.delimitedContent(persist));
            }
            this.csvStream.write(this.recordDelim);
            this.csvStream.write(content.join(this.columnDelim));
            this.rowIndex++;
            return true;
        }
        return false;
    }
}

export interface RelationalCsvTableNames {
    readonly suppliers: string;
    readonly periodicals: string;
    readonly periodicalAnchors: string;
    readonly periodicalCommonAnchors: string;
    readonly periodicalEditions: string;
    readonly editionAnchors: string;
}

export interface RelationalCsvTableWriters {
    readonly names: RelationalCsvTableNames,
    readonly suppliers: TabularWriter<pd.PeriodicalSupplier>;
    readonly periodicals: TabularWriter<pd.Periodical>;
    readonly periodicalAnchors: TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalCommonAnchors: TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalEditions: TabularWriter<pd.PeriodicalEdition>;
    readonly editionAnchors: TabularWriter<pd.ClassifiedAnchor>;
    close(): void;
}

export class DefaultRelationalCsvTableWriters implements RelationalCsvTableWriters {
    static readonly UUID_NAMESPACE: UUID = "3438161e-47a2-415d-8fc8-ae8ed80a7c86";
    static readonly NAMES: RelationalCsvTableNames = {
        suppliers: "suppliers.csv",
        periodicals: "periodicals.csv",
        periodicalAnchors: "periodical-anchors.csv",
        periodicalCommonAnchors: "periodical-anchors-common.csv",
        periodicalEditions: "periodical-editions.csv",
        editionAnchors: "periodical-edition-anchors.csv"
    }

    readonly names: RelationalCsvTableNames;
    readonly suppliers: TabularWriter<pd.PeriodicalSupplier>;
    readonly periodicals: TabularWriter<pd.Periodical>;
    readonly periodicalAnchors: TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalCommonAnchors: TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalEditions: TabularWriter<pd.PeriodicalEdition>;
    readonly editionAnchors: TabularWriter<pd.ClassifiedAnchor>;

    constructor(destPath: string, uuidNamespace = DefaultRelationalCsvTableWriters.UUID_NAMESPACE, writers?: Partial<RelationalCsvTableWriters>) {
        this.names = writers?.names || DefaultRelationalCsvTableWriters.NAMES;
        this.suppliers = writers?.suppliers || new TabularWriter({ destPath, fileName: this.names.suppliers, parentUuidNamespace: uuidNamespace });
        this.periodicals = writers?.periodicals || new TabularWriter({ destPath, fileName: this.names.periodicals, parentUuidNamespace: uuidNamespace });
        this.periodicalAnchors = writers?.periodicalAnchors || new TabularWriter({ destPath, fileName: this.names.periodicalAnchors, parentUuidNamespace: uuidNamespace });
        this.periodicalCommonAnchors = writers?.periodicalCommonAnchors || new TabularWriter({ destPath, fileName: this.names.periodicalCommonAnchors, parentUuidNamespace: uuidNamespace });
        this.periodicalEditions = writers?.periodicalEditions || new TabularWriter({ destPath, fileName: this.names.periodicalEditions, parentUuidNamespace: uuidNamespace });
        this.editionAnchors = writers?.editionAnchors || new TabularWriter({ destPath, fileName: this.names.editionAnchors, parentUuidNamespace: uuidNamespace });
    }

    close(): void {
        this.editionAnchors.close();
        this.periodicalEditions.close();
        this.periodicalCommonAnchors.close();
        this.periodicalAnchors.close();
        this.periodicals.close();
        this.suppliers.close();
    }

    static recreateDir(destPath: string): void {
        fs.rmdirSync(destPath, { recursive: true })
        fs.mkdirSync(destPath, { recursive: true });
    }

    static mkDir(destPath: string): void {
        if (!fs.existsSync(destPath)) {
            fs.mkdirSync(destPath, { recursive: true });
        }
    }
}

export class PersistRelationalCSV {
    readonly stats = {
        written: {
            suppliers: 0,
            periodicals: 0,
            periodicalAnchors: 0,
            periodicalEditions: 0,
            editionAnchors: 0,
        }
    }

    constructor(readonly writers: RelationalCsvTableWriters) {
    }

    persistSuppliers(suppliers: pd.PeriodicalSuppliers): void {
        for (const supplier of Object.values(suppliers).sort((left, right) => { return left.name.localeCompare(right.name) })) {
            this.persistSupplier(supplier);
        }
    }

    persistSupplier(supplier: pd.PeriodicalSupplier): UUID | undefined {
        const suppliersPK = this.writers.suppliers.createId(supplier.name);
        if (this.writers.suppliers.write({
            persist: { id: suppliersPK, name: supplier.name, periodicals: Object.keys(supplier.periodicals).length },
            source: supplier
        })) {
            this.stats.written.suppliers++;
            // sort entries so that repeated runs create the same order (and diff'ing is easier)
            for (const p of Object.values(supplier.periodicals).sort((left, right) => { return left.name.localeCompare(right.name) })) {
                this.persistPeriodical(suppliersPK, p);
            }
            return suppliersPK;
        }
        return undefined;
    }

    persistPeriodical(suppliersPK: string, p: pd.Periodical): UUID | undefined {
        const periodicalsPK = this.writers.periodicals.createId(p.name);
        if (this.writers.periodicals.write({
            persist: { id: periodicalsPK, supplier_id: suppliersPK, name: p.name, editions: p.editions.length },
            source: p
        })) {
            this.stats.written.periodicals++;
            this.persistAnchors(suppliersPK, periodicalsPK, p);
            return periodicalsPK;
        }
        return undefined;
    }

    persistAnchors(suppliersPK: string, periodicalsPK: string, p: pd.Periodical): void {
        // sort entries by anchor text so that repeated runs create the same order (and diff'ing is easier)
        for (const cea of Object.values(p.classifiedAnchors).sort((left, right) => { return left.anchorText.localeCompare(right.anchorText); })) {
            const periodicalAnchorPK = this.writers.periodicalAnchors.createId(periodicalsPK + cea.anchorText + cea.classification);
            const record = {
                persist: {
                    id: periodicalAnchorPK,
                    periodical_id: periodicalsPK,
                    periodical_name: p.name,
                    anchor_text_classified: cea.anchorText,
                    anchors_count: cea.count,
                    editions_count: p.editions.length,
                    classification: cea.classification.classificationID,
                    common_anchor: pd.isPeriodicalCommonAnchor(cea) ? 1 : 0,
                }, source: cea
            };
            if (this.writers.periodicalAnchors.write(record)) {
                this.stats.written.periodicalAnchors++;
                if (pd.isPeriodicalCommonAnchor(cea)) {
                    delete record.persist.common_anchor;
                    this.writers.periodicalCommonAnchors.write(record);
                }
            }
        }

        // sort by oldest edition first so that the newest goes to the end (for future diff'ing ease)
        for (const pe of p.editions.sort((left, right) => { return right.date.valueOf() - left.date.valueOf(); })) {
            const periodicalEditionsPK = this.writers.periodicalEditions.createId(suppliersPK + pe.supplierContentId);
            if (this.writers.periodicalEditions.write({
                persist: {
                    id: periodicalEditionsPK,
                    periodical_id: periodicalsPK,
                    supplier_content_id: pe.supplierContentId,
                    periodical_name: p.name,
                    from_address: pe.fromAddress,
                    from_name: pe.fromName,
                    date: pe.date.toISOString(),
                    anchors: pe.anchors.length,
                }, source: pe
            })) {
                this.stats.written.periodicalEditions++;
                pe.anchors.sort((left, right) => {
                    const ctCompare = left.classifierText.localeCompare(right.classifierText);
                    return ctCompare == 0 ? (left.href.localeCompare(right.href)) : ctCompare;
                }).forEach((ca) => {
                    const peAnchorsPK = this.writers.editionAnchors.createId(periodicalEditionsPK + ca.classifierText + ca.href);
                    if (this.writers.editionAnchors.write({
                        persist: {
                            id: peAnchorsPK,
                            edition_id: periodicalEditionsPK,
                            periodical_name: p.name,
                            date: pe.date.toISOString(),
                            classification: ca.classification.classificationID,
                            anchor_text_classified: ca.classifierText,
                            common_anchor: ca.classifiedBy ? (pd.isPeriodicalCommonAnchor(ca.classifiedBy) ? 1 : 0) : 0,
                            href: ca.href,
                        }, source: ca
                    })) {
                        this.stats.written.editionAnchors++;
                    }
                });
            }
        }
    }
}
